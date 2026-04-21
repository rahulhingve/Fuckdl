"""
MonaLisa CDM - WASM-based Content Decryption Module wrapper.
Compatible with wasmtime 41.0.0+
"""
import base64
import hashlib
import json
import logging
import re
import struct
import uuid
from pathlib import Path
from typing import Dict, Optional, Union
import wasmtime

logger = logging.getLogger(__name__)

class MonaLisaCDM:
    """
    MonaLisa CDM wrapper for WASM-based key extraction and segment decryption.
    Compatible with wasmtime 41.0.0+
    """
    # Memory constants for WASM interaction
    DYNAMIC_BASE = int(6065008)
    DYNAMICTOP_PTR = int(821968)
    LICENSE_KEY_OFFSET = int(0x5C8C0C)  # 6065164
    LICENSE_KEY_LENGTH = int(16)
    
    # Segment decryption constants
    SEGMENT_BUFFER_OFFSET = int(0x600000)
    SEGMENT_OUTPUT_OFFSET = int(0x610000)

    ENV_STRINGS = (
        "USER=web_user",
        "LOGNAME=web_user",
        "PATH=/",
        "PWD=/",
        "HOME=/home/web_user",
        "LANG=zh_CN.UTF-8",
        "_=./this.program",
    )

    def __init__(self, device_path: Path):
        """
        Initialize the MonaLisa CDM.
        Args:
            device_path: Path to the device file (.mld).
        """
        device_path = Path(device_path)
        self.device_path = device_path
        self.base_dir = device_path.parent
        
        if not self.device_path.is_file():
            raise FileNotFoundError(f"Device file not found at: {self.device_path}")

        try:
            data = json.loads(self.device_path.read_text(encoding="utf-8", errors="replace"))
        except Exception as e:
            raise ValueError(f"Invalid device file (JSON): {e}")

        wasm_path_str = data.get("wasm_path")
        if not wasm_path_str:
            raise ValueError("Device file missing 'wasm_path'")

        # Resolve WASM path relative to Fuckdl root or binaries folder
        wasm_filename = Path(wasm_path_str).name
        # Try common locations
        possible_paths = [
            self.base_dir / wasm_filename,
            self.base_dir.parent / "binaries" / wasm_filename,
            Path(__file__).parent.parent / "binaries" / wasm_filename,
        ]
        
        wasm_path = None
        for p in possible_paths:
            if p.exists():
                wasm_path = p
                break
                
        if not wasm_path:
            raise FileNotFoundError(f"WASM file not found in any of: {[str(p) for p in possible_paths]}")

        try:
            self.engine = wasmtime.Engine()
            if wasm_path.suffix.lower() == ".wat":
                self.module = wasmtime.Module.from_file(self.engine, str(wasm_path))
            else:
                self.module = wasmtime.Module(self.engine, wasm_path.read_bytes())
        except Exception as e:
            raise RuntimeError(f"Failed to load WASM module: {e}")

        self.store = None
        self.memory = None
        self.instance = None
        self.exports = {}
        self.ctx = None
        self._session_open = False
        
        # Cache for extracted keys
        self._cached_key = None
        self._cached_kid = None

    def open(self) -> int:
        """
        Open a CDM session.
        Returns:
            Session ID (always 1 for MonaLisa).
        Raises:
            RuntimeError: If session initialization fails.
        """
        if self._session_open:
            return 1

        try:
            self.store = wasmtime.Store(self.engine)
            
            # Create Memory (Updated for newer wasmtime)
            limits = wasmtime.Limits(min=256, max=256)
            memory_type = wasmtime.MemoryType(limits=limits)
            self.memory = wasmtime.Memory(self.store, memory_type)
            
            self._write_i32(int(self.DYNAMICTOP_PTR), int(self.DYNAMIC_BASE))

            imports = self._build_imports()
            self.instance = wasmtime.Instance(self.store, self.module, imports)
            ex = self.instance.exports(self.store)

            # Map exports with fallbacks for different WASM versions
            self.exports = {
                "___wasm_call_ctors": ex.get("s"),
                "_monalisa_context_alloc": ex.get("D"),
                "monalisa_set_license": ex.get("F"),
                "monalisa_decrypt_segment": ex.get("monalisa_decrypt_segment"),
                "monalisa_process": ex.get("monalisa_process"),
                "stackAlloc": ex.get("N"),
                "stackSave": ex.get("L"),
                "stackRestore": ex.get("M"),
            }

            # Fallbacks for common export name variations
            if not self.exports["___wasm_call_ctors"]:
                for name in ["__wasm_call_ctors", "_initialize", "__init"]:
                    if name in ex:
                        self.exports["___wasm_call_ctors"] = ex[name]
                        break
            
            if not self.exports["_monalisa_context_alloc"]:
                for name in ["monalisa_context_alloc", "_context_alloc", "context_alloc"]:
                    if name in ex:
                        self.exports["_monalisa_context_alloc"] = ex[name]
                        break

            if not self.exports["monalisa_set_license"]:
                for name in ["set_license", "_set_license", "license_set"]:
                    if name in ex:
                        self.exports["monalisa_set_license"] = ex[name]
                        break

            # Call constructors if they exist
            if self.exports["___wasm_call_ctors"]:
                self.exports["___wasm_call_ctors"](self.store)

            # Create context
            if self.exports["_monalisa_context_alloc"]:
                ctx_val = self.exports["_monalisa_context_alloc"](self.store)
                # In newer wasmtime, results might be wrapped or direct ints
                self.ctx = int(ctx_val) if hasattr(ctx_val, '__int__') else ctx_val
            else:
                self.ctx = 1

            self._session_open = True
            return 1

        except Exception as e:
            self.close()
            raise RuntimeError(f"Failed to initialize session: {e}") from e

    def close(self, session_id: int = 1) -> None:
        """Close the CDM session and release resources."""
        self.store = None
        self.memory = None
        self.instance = None
        self.exports = {}
        self.ctx = None
        self._session_open = False

    def extract_keys(self, license_data: Union[str, bytes]) -> Dict:
        """
        Extract decryption keys from license/ticket data.
        """
        if not self._session_open:
            logger.debug("Session not open, calling open() automatically...")
            self.open()

        if not self.instance or not self.memory:
            raise RuntimeError("Session not open. Call open() first.")

        if not license_data:
            raise ValueError("license_data is empty")

        if isinstance(license_data, bytes):
            license_b64 = base64.b64encode(license_data).decode("utf-8")
        else:
            license_b64 = license_data

        # Call monalisa_set_license
        if self.exports.get("monalisa_set_license"):
            ret = self._ccall(
                "monalisa_set_license",
                int,
                self.ctx,
                license_b64,
                len(license_b64),
                "0",
            )
        else:
            # Fallback: search for alternative function names
            func_name = None
            for name in ["set_license", "_set_license", "license_set", "process_license"]:
                if self.exports.get(name):
                    func_name = name
                    break
            if not func_name:
                raise RuntimeError("No license processing function found in WASM")
            
            ret = self._ccall(func_name, int, self.ctx, license_b64, len(license_b64))

        if ret != 0:
            raise RuntimeError(f"License validation failed with code: {ret}")

        key_bytes = self._extract_license_key_bytes()
        self._cached_key = key_bytes.hex()

        # Extract DCID from license to generate KID
        try:
            decoded = base64.b64decode(license_b64).decode("ascii", errors="ignore")
        except Exception:
            decoded = ""

        m = re.search(
            r"DCID-[A-Z0-9]+-[A-Z0-9]+-\d{8}-\d{6}-[A-Z0-9]+-\d{10}-[A-Z0-9]+",
            decoded,
        )
        if m:
            kid_bytes = uuid.uuid5(uuid.NAMESPACE_DNS, m.group()).bytes
            self._cached_kid = kid_bytes.hex()
        else:
            license_hash = hashlib.sha256(license_b64.encode()).hexdigest()
            kid_bytes = uuid.uuid5(uuid.NAMESPACE_DNS, f"monalisa:license:{license_hash}").bytes
            self._cached_kid = kid_bytes.hex()

        return {"kid": self._cached_kid, "key": self._cached_key, "type": "CONTENT"}

    def get_decryption_key(self) -> bytes:
        """Return the raw decryption key bytes for AES decryption."""
        if self._cached_key:
            return bytes.fromhex(self._cached_key)
        return self._extract_license_key_bytes()

    def decrypt_segment(self, encrypted_data: bytes, key_hex: str = None, kid=None) -> bytes:
        """
        Decrypt a segment using the WASM module or native fallback.
        """
        if not self._session_open:
            self.open()

        if not self.instance or not self.memory:
            raise RuntimeError("Session not open. Call open() first.")

        key_bytes = self.get_decryption_key() if not key_hex else bytes.fromhex(key_hex)

        # Method 1: Use specific WASM decryption function if available
        if self.exports.get("monalisa_decrypt_segment"):
            try:
                return self._decrypt_with_wasm_func(encrypted_data, key_bytes)
            except Exception as e:
                logger.debug(f"WASM decryption failed: {e}. Falling back to native.")

        # Method 2: Use monalisa_process if available
        if self.exports.get("monalisa_process"):
            try:
                return self._decrypt_with_process_func(encrypted_data, key_bytes)
            except Exception as e:
                logger.debug(f"WASM process failed: {e}. Falling back to native.")

        # Method 3: Native AES-CTR fallback (Most reliable for iQIYI)
        logger.debug("Using native AES-CTR fallback for decryption")
        return self._decrypt_native_aes_ctr(encrypted_data, key_bytes)

    def _decrypt_with_wasm_func(self, encrypted_data: bytes, key_bytes: bytes) -> bytes:
        """Decrypt using WASM's monalisa_decrypt_segment function."""
        # Write encrypted data to memory
        data_ptr = self._write_bytes_to_memory(encrypted_data)
        # Write key to memory
        key_ptr = self._write_bytes_to_memory(key_bytes)
        
        # Allocate space for output size
        output_size_ptr = self.exports["stackAlloc"](self.store, 4)
        
        # Call function
        result_ptr = self._ccall(
            "monalisa_decrypt_segment",
            int,
            data_ptr,
            len(encrypted_data),
            key_ptr,
            output_size_ptr
        )
        
        # Read output size
        output_size = self._read_i32(output_size_ptr)
        
        # Read decrypted data
        decrypted_data = self._read_bytes_from_memory(result_ptr, output_size)
        return decrypted_data

    def _decrypt_with_process_func(self, encrypted_data: bytes, key_bytes: bytes) -> bytes:
        """Decrypt using WASM's monalisa_process function."""
        data_b64 = base64.b64encode(encrypted_data).decode("ascii")
        ret = self._ccall(
            "monalisa_process",
            int,
            self.ctx,
            data_b64,
            len(data_b64),
            "1",  # flag for segment
        )
        if ret != 0:
            raise RuntimeError(f"Segment processing failed with code: {ret}")
        
        # Assuming result is placed in standard output buffer or similar
        # This depends heavily on the specific WASM implementation
        return self._extract_license_key_bytes() 

    def _decrypt_native_aes_ctr(self, encrypted_data: bytes, key_bytes: bytes) -> bytes:
        """
        Native AES-CTR decryption fallback.
        """
        from Crypto.Cipher import AES
        from Crypto.Util import Counter

        iv = None
        # Method 1: First 16 bytes might be the IV
        if len(encrypted_data) >= 16:
            potential_iv = encrypted_data[:16]
            # Check if it looks like a valid IV (not all zeros or FFs)
            if any(b not in (0, 0xFF) for b in potential_iv):
                iv = potential_iv
                encrypted_data = encrypted_data[16:]

        # Method 2: Default IV (zeros)
        if iv is None:
            iv = bytes(16)

        # Create CTR cipher
        iv_int = int.from_bytes(iv, 'big')
        ctr = Counter.new(128, initial_value=iv_int, little_endian=False)
        cipher = AES.new(key_bytes, AES.MODE_CTR, counter=ctr)
        
        try:
            decrypted = cipher.decrypt(encrypted_data)
            return decrypted
        except Exception as e:
            raise RuntimeError(f"AES-CTR decryption failed: {e}")

    def _extract_license_key_bytes(self) -> bytes:
        """Extract the 16-byte decryption key from WASM memory."""
        data_ptr = self.memory.data_ptr(self.store)
        data_len = int(self.memory.data_len(self.store))
        
        if int(self.LICENSE_KEY_OFFSET) + int(self.LICENSE_KEY_LENGTH) > data_len:
            raise RuntimeError("License key offset beyond memory bounds")

        import ctypes
        mem_ptr = ctypes.cast(data_ptr, ctypes.POINTER(ctypes.c_ubyte * data_len))
        start = int(self.LICENSE_KEY_OFFSET)
        end = int(self.LICENSE_KEY_OFFSET + self.LICENSE_KEY_LENGTH)
        return bytes(mem_ptr.contents[start:end])

    def _ccall(self, func_name: str, return_type: type, *args):
        """Call a WASM function with automatic string conversion."""
        import sys
        stack = 0
        converted_args = []
        
        try:
            for arg in args:
                if isinstance(arg, str):
                    if stack == 0:
                        if self.exports.get("stackSave"):
                            stack = self.exports["stackSave"](self.store)
                    
                    max_length = (len(arg) << 2) + 1
                    if self.exports.get("stackAlloc"):
                        ptr = self.exports["stackAlloc"](self.store, max_length)
                        self._string_to_utf8(arg, ptr, max_length)
                        converted_args.append(ptr)
                    else:
                        converted_args.append(arg)
                else:
                    converted_args.append(arg)

            func = self.exports.get(func_name)
            if not func:
                raise RuntimeError(f"Function {func_name} not found in exports")
            
            result = func(self.store, *converted_args)
            
        finally:
            if stack != 0 and self.exports.get("stackRestore"):
                try:
                    self.exports["stackRestore"](self.store, stack)
                except Exception:
                    pass

        if return_type is bool:
            return bool(result)
        return result

    def _write_i32(self, addr: int, value: int) -> None:
        """Write a 32-bit integer to WASM memory."""
        import ctypes
        data_len = int(self.memory.data_len(self.store))
        if addr < 0 or addr + 4 > data_len:
            raise IndexError(f"i32 write out of bounds: addr={addr}, mem_len={data_len}")
        
        data = self.memory.data_ptr(self.store)
        mem_ptr = ctypes.cast(data, ctypes.POINTER(ctypes.c_int32))
        mem_ptr[addr >> 2] = value

    def _string_to_utf8(self, data: str, ptr: int, max_length: int) -> int:
        """Convert string to UTF-8 and write to WASM memory."""
        import ctypes
        encoded = data.encode("utf-8")
        write_length = min(len(encoded), max_length - 1)
        
        mem_data = self.memory.data_ptr(self.store)
        mem_ptr = ctypes.cast(mem_data, ctypes.POINTER(ctypes.c_ubyte))
        
        for i in range(write_length):
            mem_ptr[ptr + i] = encoded[i]
        mem_ptr[ptr + write_length] = 0
        return write_length

    def _write_bytes_to_memory(self, data: bytes, ptr: int = None) -> int:
        """Write bytes to WASM memory and return pointer."""
        import ctypes
        if ptr is None and self.exports.get("stackAlloc"):
            ptr = self.exports["stackAlloc"](self.store, len(data) + 16)
        elif ptr is None:
            ptr = self.SEGMENT_BUFFER_OFFSET
            
        mem_data = self.memory.data_ptr(self.store)
        mem_ptr = ctypes.cast(mem_data, ctypes.POINTER(ctypes.c_ubyte))
        
        for i, byte_val in enumerate(data):
            mem_ptr[ptr + i] = byte_val
        return ptr

    def _read_bytes_from_memory(self, ptr: int, length: int) -> bytes:
        """Read bytes from WASM memory."""
        import ctypes
        mem_data = self.memory.data_ptr(self.store)
        mem_ptr = ctypes.cast(mem_data, ctypes.POINTER(ctypes.c_ubyte))
        return bytes(mem_ptr[ptr:ptr + length])

    def _read_i32(self, ptr: int) -> int:
        """Read 32-bit integer from WASM memory."""
        import ctypes
        mem_data = self.memory.data_ptr(self.store)
        mem_ptr = ctypes.cast(mem_data, ctypes.POINTER(ctypes.c_int32))
        return mem_ptr[ptr >> 2]

    def _build_imports(self):
        """Build the WASM import stubs required by the MonaLisa module."""
        import ctypes

        def sys_fcntl64(a, b, c): return 0
        def fd_write(a, b, c, d): return 0
        def fd_close(a): return 0
        def sys_ioctl(a, b, c): return 0
        def sys_open(a, b, c): return 0
        def sys_rmdir(a): return 0
        def sys_unlink(a): return 0
        def clock(): return 0
        def time(a): return 0
        def emscripten_run_script(a): return None
        def fd_seek(a, b, c, d, e): return 0
        def emscripten_resize_heap(a): return 0
        def fd_read(a, b, c, d): return 0
        def emscripten_run_script_string(a): return 0
        def emscripten_run_script_int(a): return 1
        
        def emscripten_memcpy_big(dest, src, num):
            mem_data = self.memory.data_ptr(self.store)
            data_len = int(self.memory.data_len(self.store))
            if num is None:
                num = data_len - 1
            mem_ptr = ctypes.cast(mem_data, ctypes.POINTER(ctypes.c_ubyte))
            for i in range(num):
                if dest + i < data_len and src + i < data_len:
                    mem_ptr[dest + i] = mem_ptr[src + i]
            return dest

        def environ_get(environ_ptr, environ_buf):
            buf_size = 0
            for index, string in enumerate(self.ENV_STRINGS):
                ptr = environ_buf + buf_size
                self._write_i32(environ_ptr + index * 4, ptr)
                self._write_ascii_to_memory(string, ptr)
                buf_size += len(string) + 1
            return 0

        def environ_sizes_get(penviron_count, penviron_buf_size):
            self._write_i32(penviron_count, len(self.ENV_STRINGS))
            buf_size = sum(len(s) + 1 for s in self.ENV_STRINGS)
            self._write_i32(penviron_buf_size, buf_size)
            return 0

        i32 = wasmtime.ValType.i32()
        
        return [
            wasmtime.Func(self.store, wasmtime.FuncType([i32, i32, i32], [i32]), sys_fcntl64),
            wasmtime.Func(self.store, wasmtime.FuncType([i32, i32, i32, i32], [i32]), fd_write),
            wasmtime.Func(self.store, wasmtime.FuncType([i32], [i32]), fd_close),
            wasmtime.Func(self.store, wasmtime.FuncType([i32, i32, i32], [i32]), sys_ioctl),
            wasmtime.Func(self.store, wasmtime.FuncType([i32, i32, i32], [i32]), sys_open),
            wasmtime.Func(self.store, wasmtime.FuncType([i32], [i32]), sys_rmdir),
            wasmtime.Func(self.store, wasmtime.FuncType([i32], [i32]), sys_unlink),
            wasmtime.Func(self.store, wasmtime.FuncType([], [i32]), clock),
            wasmtime.Func(self.store, wasmtime.FuncType([i32], [i32]), time),
            wasmtime.Func(self.store, wasmtime.FuncType([i32], []), emscripten_run_script),
            wasmtime.Func(self.store, wasmtime.FuncType([i32, i32, i32, i32, i32], [i32]), fd_seek),
            wasmtime.Func(self.store, wasmtime.FuncType([i32, i32, i32], [i32]), emscripten_memcpy_big),
            wasmtime.Func(self.store, wasmtime.FuncType([i32], [i32]), emscripten_resize_heap),
            wasmtime.Func(self.store, wasmtime.FuncType([i32, i32], [i32]), environ_get),
            wasmtime.Func(self.store, wasmtime.FuncType([i32, i32], [i32]), environ_sizes_get),
            wasmtime.Func(self.store, wasmtime.FuncType([i32, i32, i32, i32], [i32]), fd_read),
            wasmtime.Func(self.store, wasmtime.FuncType([i32], [i32]), emscripten_run_script_string),
            wasmtime.Func(self.store, wasmtime.FuncType([i32], [i32]), emscripten_run_script_int),
            self.memory,
        ]

    def _write_ascii_to_memory(self, string: str, buffer: int, dont_add_null: int = 0) -> None:
        """Write ASCII string to WASM memory."""
        import ctypes
        buffer = int(buffer)
        mem_data = self.memory.data_ptr(self.store)
        mem_ptr = ctypes.cast(mem_data, ctypes.POINTER(ctypes.c_ubyte))
        encoded = string.encode("utf-8")
        for i, byte_val in enumerate(encoded):
            mem_ptr[buffer + i] = byte_val
        if dont_add_null == 0:
            mem_ptr[buffer + len(encoded)] = 0