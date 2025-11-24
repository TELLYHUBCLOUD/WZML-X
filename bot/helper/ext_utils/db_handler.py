from importlib import import_module

from aiofiles import open as aiopen
from aiofiles.os import path as aiopath
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError
from pymongo.server_api import ServerApi

from ... import LOGGER, qbit_options, rss_dict, user_data
from ...core.config_manager import Config
from ...core.tg_client import TgClient


class DbManager:
    def __init__(self):
        self._return = True
        self._conn = None

    @property
    def db(self):
        """Dynamically returns the correct database based on current TgClient.ID"""
        if self._conn is None:
            return None
        db_id = TgClient.ID if TgClient.ID else "default"
        return self._conn[f"tellywzmlx{db_id}"]

    async def connect(self):
        try:
            if self._conn is not None:
                await self._conn.close()
            self._conn = AsyncIOMotorClient(
                Config.DATABASE_URL, server_api=ServerApi("1")
            )
            self._return = False
            LOGGER.info("Successfully connected to the database.")
        except PyMongoError as e:
            LOGGER.error(f"Error in DB connection: {e}")
            self._conn = None
            self._return = True

    async def disconnect(self):
        self._return = True
        if self._conn is not None:
            await self._conn.close()
            LOGGER.info("Database connection closed.")
        self._conn = None

    async def update_deploy_config(self):
        if self._return:
            return
        settings = import_module("config")
        config_file = {
            key: value.strip() if isinstance(value, str) else value
            for key, value in vars(settings).items()
            if not key.startswith("__")
        }
        await self.db.settings.deployConfig.replace_one(
            {"_id": TgClient.ID}, config_file, upsert=True
        )

    async def update_config(self, dict_):
        if self._return:
            return
        await self.db.settings.config.update_one(
            {"_id": TgClient.ID}, {"$set": dict_}, upsert=True
        )

    async def update_aria2(self, key, value):
        if self._return:
            return
        await self.db.settings.aria2c.update_one(
            {"_id": TgClient.ID}, {"$set": {key: value}}, upsert=True
        )

    async def update_qbittorrent(self, key, value):
        if self._return:
            return
        await self.db.settings.qbittorrent.update_one(
            {"_id": TgClient.ID}, {"$set": {key: value}}, upsert=True
        )

    async def save_qbit_settings(self):
        if self._return:
            return
        await self.db.settings.qbittorrent.update_one(
            {"_id": TgClient.ID}, {"$set": qbit_options}, upsert=True
        )

    async def update_private_file(self, path):
        if self._return:
            return
        db_path = path.replace(".", "__")
        if await aiopath.exists(path):
            async with aiopen(path, "rb+") as pf:
                pf_bin = await pf.read()
            await self.db.settings.files.update_one(
                {"_id": TgClient.ID}, {"$set": {db_path: pf_bin}}, upsert=True
            )
            if path == "config.py":
                await self.update_deploy_config()
        else:
            await self.db.settings.files.update_one(
                {"_id": TgClient.ID}, {"$unset": {db_path: ""}}, upsert=True
            )

    async def update_nzb_config(self):
        if self._return:
            return
        async with aiopen("sabnzbd/SABnzbd.ini", "rb+") as pf:
            nzb_conf = await pf.read()
        await self.db.settings.nzb.replace_one(
            {"_id": TgClient.ID}, {"SABnzbd__ini": nzb_conf}, upsert=True
        )

    async def update_user_data(self, user_id):
        if self._return:
            return
        data = user_data.get(user_id, {})
        data = data.copy()
        for key in ("THUMBNAIL", "RCLONE_CONFIG", "TOKEN_PICKLE", "USER_COOKIE_FILE"):
            data.pop(key, None)
        pipeline = [
            {
                "$replaceRoot": {
                    "newRoot": {
                        "$mergeObjects": [
                            data,
                            {
                                "$arrayToObject": {
                                    "$filter": {
                                        "input": {"$objectToArray": "$$ROOT"},
                                        "as": "field",
                                        "cond": {
                                            "$in": [
                                                "$$field.k",
                                                [
                                                    "THUMBNAIL",
                                                    "RCLONE_CONFIG",
                                                    "TOKEN_PICKLE",
                                                    "USER_COOKIE_FILE",
                                                ],
                                            ]
                                        },
                                    }
                                }
                            },
                        ]
                    }
                }
            }
        ]
        await self.db.users.update_one({"_id": user_id}, pipeline, upsert=True)

    async def update_user_doc(self, user_id, key, path=""):
        if self._return:
            return
        if path:
            async with aiopen(path, "rb+") as doc:
                doc_bin = await doc.read()
            await self.db.users.update_one(
                {"_id": user_id}, {"$set": {key: doc_bin}}, upsert=True
            )
        else:
            await self.db.users.update_one(
                {"_id": user_id}, {"$unset": {key: ""}}, upsert=True
            )

    async def rss_update_all(self):
        if self._return:
            return
        for user_id in list(rss_dict.keys()):
            await self.db.rss.replace_one(
                {"_id": user_id}, rss_dict[user_id], upsert=True
            )

    async def rss_update(self, user_id):
        if self._return:
            return
        await self.db.rss.replace_one(
            {"_id": user_id}, rss_dict[user_id], upsert=True
        )

    async def rss_delete(self, user_id):
        if self._return:
            return
        await self.db.rss.delete_one({"_id": user_id})

    async def add_incomplete_task(self, cid, link, tag):
        if self._return:
            return
        await self.db.tasks.insert_one({"_id": link, "cid": cid, "tag": tag})

    async def get_pm_uids(self):
        if self._return:
            return []
        return [doc["_id"] async for doc in self.db.pm_users.find({})]

    async def set_pm_users(self, user_id):
        if self._return:
            return
        if not bool(await self.db.pm_users.find_one({"_id": user_id})):
            await self.db.pm_users.insert_one({"_id": user_id})
            LOGGER.info(f"New PM User Added : {user_id}")

    async def rm_pm_user(self, user_id):
        if self._return:
            return
        await self.db.pm_users.delete_one({"_id": user_id})

    async def rm_complete_task(self, link):
        if self._return:
            return
        await self.db.tasks.delete_one({"_id": link})

    async def get_incomplete_tasks(self):
        notifier_dict = {}
        if self._return:
            return notifier_dict
        if await self.db.tasks.find_one():
            rows = self.db.tasks.find({})
            async for row in rows:
                if row["cid"] in list(notifier_dict.keys()):
                    if row["tag"] in list(notifier_dict[row["cid"]]):
                        notifier_dict[row["cid"]][row["tag"]].append(row["_id"])
                    else:
                        notifier_dict[row["cid"]][row["tag"]] = [row["_id"]]
                else:
                    notifier_dict[row["cid"]] = {row["tag"]: [row["_id"]]}
        await self.db.tasks.drop()
        return notifier_dict

    async def trunc_table(self, name):
        if self._return:
            return
        await self.db[name].drop()


database = DbManager()
