import os
import json
import sqlite3
import logging
import traceback
import requests
from enum import Enum

import pymysql

from fuckdl.utils.AtomicSQL import AtomicSQL
from fuckdl.utils.collections import first_or_none


log = logging.getLogger("Vaults")

class InsertResult(Enum):
    FAILURE = 0
    SUCCESS = 1
    ALREADY_EXISTS = 2


class Vault:
    """
    Key Vault.
    This defines various details about the vault, including its Connection object.
    """

    def __init__(
        self,
        type_,
        name,
        ticket=None,
        path=None,
        username=None,
        password=None,
        database=None,
        host=None,
        port=3306,
        method="GET",
    ):
        from fuckdl.config import directories

        try:
            self.type = self.Types[type_.upper()]
        except KeyError:
            raise ValueError(f"Invalid vault type [{type_}]")
        self.name = name
        self.con = None
        self.method = method.upper()
        
        if self.type == Vault.Types.LOCAL:
            if not path:
                raise ValueError("Local vault has no path specified")
            self.con = sqlite3.connect(
                os.path.expanduser(path).format(data_dir=directories.data)
            )
        elif self.type == Vault.Types.REMOTE:
            self.con = pymysql.connect(
                user=username,
                password=password or "",
                db=database,
                host=host,
                port=port,
                cursorclass=pymysql.cursors.DictCursor,
            )
        elif self.type == Vault.Types.HTTP:
            self.url = host
            self.username = username
            self.password = password
            if self.method not in ["GET", "POST"]:
                log.warning(f"HTTP method not supported: {self.method}, using GET by default")
                self.method = "GET"
        elif self.type == Vault.Types.HTTPAPI:
            self.url = host
            self.password = password
            # Ensure URL ends with slash for DRMLab compatibility
            if self.url and not self.url.endswith('/'):
                self.url = self.url + '/'
        else:
            raise ValueError(f"Invalid vault type [{self.type.name}]")
            
        self.ph = {
            self.Types.LOCAL: "?",
            self.Types.REMOTE: "%s",
            self.Types.HTTP: None,
            self.Types.HTTPAPI: None,
        }[self.type]
        self.ticket = ticket

        self.perms = self.get_permissions()
        if not self.has_permission("SELECT"):
            raise ValueError(
                f"Cannot use vault. Vault {self.name} has no SELECT permission."
            )

    def __str__(self):
        return f"{self.name} ({self.type.name})"

    def get_permissions(self):
        if self.type == self.Types.LOCAL:
            return [tuple([["*"], tuple(["*", "*"])])]
        elif self.type == self.Types.HTTP:
            return [tuple([["*"], tuple(["*", "*"])])]
        elif self.type == self.Types.HTTPAPI:
            return [tuple([["*"], tuple(["*", "*"])])]

        with self.con.cursor() as c:
            c.execute("SHOW GRANTS")
            grants = c.fetchall()
            grants = [next(iter(x.values())) for x in grants]
        grants = [tuple(x[6:].split(" TO ")[0].split(" ON ")) for x in list(grants)]
        grants = [
            (
                list(map(str.strip, perms.replace("ALL PRIVILEGES", "*").split(","))),
                location.replace("`", "").split("."),
            )
            for perms, location in grants
        ]

        return grants

    def has_permission(self, operation, database=None, table=None):
        grants = [x for x in self.perms if x[0] == ["*"] or operation.upper() in x[0]]
        if grants and database:
            grants = [x for x in grants if x[1][0] in (database, "*")]
        if grants and table:
            grants = [x for x in grants if x[1][1] in (table, "*")]
        return bool(grants)

    class Types(Enum):
        LOCAL = 1
        REMOTE = 2
        HTTP = 3
        HTTPAPI = 4


class Vaults:
    """
    Key Vaults.
    Keeps hold of Vault objects, with convenience functions for
    using multiple vaults in one actions, e.g. searching vaults
    for a key based on kid.
    This object uses AtomicSQL for accessing the vault connections
    instead of directly. This is to provide thread safety but isn't
    strictly necessary.
    """

    def __init__(self, vaults, service):
        self.adb = AtomicSQL()
        self.api_session_id = None
        self.vaults = sorted(
            vaults, key=lambda v: 0 if v.type is Vault.Types.LOCAL else 1
        )
        self.service = service.lower()
        for vault in self.vaults:
            if vault.type is Vault.Types.HTTP or vault.type is Vault.Types.HTTPAPI:
                continue

            vault.ticket = self.adb.load(vault.con)
            self.create_table(vault, self.service, commit=True)

    def request(self, method, url, key, params=None):
        """
        Make a JSON-RPC request to HTTPAPI vault (e.g., DRMLab)
        """
        # Ensure URL ends with slash for DRMLab compatibility
        if not url.endswith('/'):
            url = url + '/'
        
        # Build payload according to DRMLab format
        payload = {
            "method": method,
            "params": {
                **(params or {}),
                "session_id": self.api_session_id,
            },
            "token": key,
        }
        
        # Remove None values from params for cleaner request
        if payload["params"]:
            payload["params"] = {k: v for k, v in payload["params"].items() if v is not None}
        
        log.debug(f"HTTPAPI Request: {method} to {url}")
        
        try:
            r = requests.post(url, json=payload, timeout=30)
        except requests.exceptions.RequestException as e:
            raise ValueError(f"Request failed: {e}")

        if not r.ok:
            raise ValueError(f"API returned HTTP Error {r.status_code}: {r.reason.title()}")

        try:
            res = r.json()
        except json.JSONDecodeError:
            raise ValueError(f"API returned an invalid response: {r.text}")

        # Handle DRMLab response format (status: "ok")
        if res.get("status") == "ok":
            return res.get("message", {})
        # Handle legacy format (status_code: 200)
        elif res.get("status_code") == 200:
            return res.get("message", {})
        else:
            raise ValueError(f"API returned an error: {res}")

    def __iter__(self):
        return iter(self.vaults)

    def _http_request(self, vault, method, **params):
        """
        Unified HTTP request handler for HTTP type vaults (not HTTPAPI)
        Supports both GET and POST methods
        """
        try:
            if vault.method == "GET":
                response = requests.get(
                    vault.url,
                    params=params,
                    timeout=30
                )
            else:  # POST
                response = requests.post(
                    vault.url,
                    json=params,
                    timeout=30
                )
            
            if not response.ok:
                log.error(f"HTTP {response.status_code} on {vault.name}: {response.reason}")
                return None
                
            return response.json()
            
        except requests.exceptions.RequestException as e:
            log.error(f"Connection error with {vault.name}: {e}")
            return None

    def get(self, kid, title):
        """
        Get a key from vaults by KID
        """
        for vault in self.vaults:
            if vault.type is Vault.Types.HTTP:
                keys_data = self._http_request(
                    vault,
                    "get",
                    service=self.service,
                    username=vault.username,
                    password=vault.password,
                    kid=kid,
                )
                
                if keys_data and keys_data.get("keys"):
                    return keys_data["keys"][0]["key"], vault
                    
            elif vault.type is Vault.Types.HTTPAPI:
                try:
                    result = self.request(
                        "GetKey",
                        vault.url,
                        vault.password,
                        {
                            "kid": kid,
                            "service": self.service,
                            "title": title,
                        },
                    )
                    
                    # Handle DRMLab response format
                    keys = result.get("keys", [])
                    if keys:
                        # Try to find exact KID match
                        for k in keys:
                            if k.get("kid", "").lower() == kid.lower():
                                return k.get("key"), vault
                        # Return first key if no exact match
                        return keys[0].get("key"), vault
                    return None, None
                    
                except (Exception, SystemExit) as e:
                    log.debug(traceback.format_exc())
                    if not isinstance(e, SystemExit):
                        log.error(f"Failed to get key ({e.__class__.__name__}: {e})")
                    return None, None
                    
            else:
                # SQL vaults (LOCAL/REMOTE)
                if not self.table_exists(vault, self.service):
                    continue
                if not vault.ticket:
                    raise ValueError(
                        f"Vault {vault.name} does not have a valid ticket available."
                    )
                c = self.adb.safe_execute(
                    vault.ticket,
                    lambda db, cursor: cursor.execute(
                        "SELECT `id`, `key_`, `title` FROM `{1}` WHERE `kid`={0}".format(
                            vault.ph, self.service
                        ),
                        [kid],
                    ),
                ).fetchone()
                if c:
                    if isinstance(c, dict):
                        c = list(c.values())
                    if not c[2] and vault.has_permission("UPDATE", table=self.service):
                        self.adb.safe_execute(
                            vault.ticket,
                            lambda db, cursor: cursor.execute(
                                "UPDATE `{1}` SET `title`={0} WHERE `id`={0}".format(
                                    vault.ph, self.service
                                ),
                                [title, c[0]],
                            ),
                        )
                        self.commit(vault)
                    return c[1], vault
        return None, None

    def table_exists(self, vault, table):
        if vault.type == Vault.Types.HTTP:
            return True
        if not vault.ticket:
            raise ValueError(
                f"Vault {vault.name} does not have a valid ticket available."
            )
        if vault.type == Vault.Types.LOCAL:
            return (
                self.adb.safe_execute(
                    vault.ticket,
                    lambda db, cursor: cursor.execute(
                        f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name={vault.ph}",
                        [table],
                    ),
                ).fetchone()[0]
                == 1
            )
        return (
            list(
                self.adb.safe_execute(
                    vault.ticket,
                    lambda db, cursor: cursor.execute(
                        f"SELECT count(TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_NAME={vault.ph}",
                        [table],
                    ),
                )
                .fetchone()
                .values()
            )[0]
            == 1
        )

    def create_table(self, vault, table, commit=False):
        if self.table_exists(vault, table):
            return
        if not vault.ticket:
            raise ValueError(
                f"Vault {vault.name} does not have a valid ticket available."
            )
        if vault.has_permission("CREATE"):
            print(f"Creating `{table}` table in {vault} key vault...")
            self.adb.safe_execute(
                vault.ticket,
                lambda db, cursor: cursor.execute(
                    "CREATE TABLE IF NOT EXISTS {} (".format(table)
                    + (
                        """
                        "id"        INTEGER NOT NULL UNIQUE,
                        "kid"       TEXT NOT NULL COLLATE NOCASE,
                        "key_"      TEXT NOT NULL COLLATE NOCASE,
                        "title"     TEXT,
                        PRIMARY KEY("id" AUTOINCREMENT),
                        UNIQUE("kid", "key_")
                        """
                        if vault.type == Vault.Types.LOCAL
                        else """
                        id          INTEGER AUTO_INCREMENT PRIMARY KEY,
                        kid         VARCHAR(255) NOT NULL,
                        key_        VARCHAR(255) NOT NULL,
                        title       TEXT,
                        UNIQUE(kid, key_)
                        """
                    )
                    + ");"
                ),
            )
            if commit:
                self.commit(vault)

    def insert_key(self, vault, table, kid, key, title, commit=False):
        """
        Insert a key into the vault
        """
        if vault.type == Vault.Types.HTTP:
            result_data = self._http_request(
                vault,
                "insert",
                service=self.service,
                username=vault.username,
                password=vault.password,
                kid=kid,
                key=key,
                title=title
            )
            
            if result_data:
                if result_data.get("status_code") == 200 and result_data.get("inserted"):
                    return InsertResult.SUCCESS
                elif result_data.get("status_code") == 200:
                    return InsertResult.ALREADY_EXISTS
            
            return InsertResult.FAILURE
            
        elif vault.type is Vault.Types.HTTPAPI:
            try:
                result = self.request(
                    "InsertKey",
                    vault.url,
                    vault.password,
                    {
                        "kid": kid,
                        "key": key,
                        "service": self.service,
                        "title": title,
                    },
                )
                
                # Check if insertion was successful
                if result.get("status") == "ok" or result.get("inserted"):
                    return InsertResult.SUCCESS
                else:
                    return InsertResult.FAILURE
                    
            except Exception as e:
                log.error(f"Failed to insert key into {vault.name}: {e}")
                return InsertResult.FAILURE
                
        # SQL vaults (LOCAL/REMOTE)
        if not self.table_exists(vault, table):
            return InsertResult.FAILURE
        if not vault.ticket:
            raise ValueError(
                f"Vault {vault.name} does not have a valid ticket available."
            )
        if not vault.has_permission("INSERT", table=table):
            raise ValueError(
                f"Cannot insert key into Vault. Vault {vault.name} has no INSERT permission."
            )
        if self.adb.safe_execute(
            vault.ticket,
            lambda db, cursor: cursor.execute(
                "SELECT `id` FROM `{1}` WHERE `kid`={0} AND `key_`={0}".format(
                    vault.ph, self.service
                ),
                [kid, key],
            ),
        ).fetchone():
            return InsertResult.ALREADY_EXISTS
        self.adb.safe_execute(
            vault.ticket,
            lambda db, cursor: cursor.execute(
                "INSERT INTO `{1}` (kid, key_, title) VALUES ({0}, {0}, {0})".format(
                    vault.ph, table
                ),
                (kid, key, title),
            ),
        )
        if commit:
            self.commit(vault)
        return InsertResult.SUCCESS

    def commit(self, vault):
        if vault.type == Vault.Types.HTTP or vault.type == Vault.Types.HTTPAPI:
            return
        self.adb.commit(vault.ticket)