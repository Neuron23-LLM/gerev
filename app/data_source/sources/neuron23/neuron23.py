import logging
from datetime import datetime
from typing import List, Dict

from pydantic import BaseModel
from data_source.api.base_data_source import BaseDataSource, BaseDataSourceConfig, ConfigField
from data_source.api.basic_document import BasicDocument, DocumentType
from queues.index_queue import IndexQueue
from data_source.api.exception import InvalidDataSourceConfig

class MagicConfig(BaseDataSourceConfig):
    url: str
    username: str
    token: str

class MagicDataSource(BaseDataSource):

    @staticmethod
    def get_config_fields() -> List[ConfigField]:
        """
        list of the config fields which should be the same fields as in MagicConfig, for dynamic UI generation
        """
        pass

    @staticmethod
    async def validate_config(config: Dict) -> None:
        """
        Validate the configuration and raise an exception if it's invalid,
        You should try to actually connect to the data source and verify that it's working
        """
        pass

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        magic_config = MagicConfig(**self._config)
        self._magic_client = MagicClient(url=magic_config.url,
                                         username=magic_config.username,
                                         token=magic_config.token)

    def _feed_new_documents(self) -> None:
        """
        Add new documents to the index queue, will explaine later
        """
        pass

    def _fetch_channel(self, channel: Channel) -> None:
        messages = self._magic_client.list_messages(channel)
        for message in messages:
            doc = BasicDocument(
                id=message["id"],
                data_source_id=self._data_source_id,
                type=DocumentType.MESSAGE,
                title=message['title'],
                content=message.get("description"),
                author=message['author']['name'],
                author_image_url=message['author']['avatar_url'],
                location=message['references']['full'],
                url=message['web_url'],
                timestamp=message['created_at'],
            )
            IndexQueue.get_instance().put_single(document)