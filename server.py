import aiohttp
import aiofiles
import argparse
from aiohttp import web
import asyncio
import os
from threading import Thread
import yaml


class StorageServer:
    def __init__(self, host, port, directory, nodes, save_file):
        self.host = host
        self.port = port
        self.directory = directory
        self.nodes = nodes
        self.save_file = save_file

    def run(self):
        app = web.Application()
        app.add_routes([
            web.get('/info', self.get_info),
            web.get('/{file_name}', self.get_file),
            web.post('/{file_name}', self.post_file),
            web.get('/from_node/{file_name}', self.get_local)
        ])

        web.run_app(app, host=self.host, port=self.port)

    async def get_info(self, *_):
        return web.json_response(data={'address': '{}:{}'.format(self.host, self.port), 'base_dir': self.directory})

    async def _read_file(self, file_name):
        """Асинхронное чтение из файла.
        Args:
            file_name (string): Имя файла.
        Returns:
            content: Содержимое файла.
        """
        path = os.path.join(self.directory, file_name)
        if os.path.exists(path):
            async with aiofiles.open(path, 'rb') as file:
                return await file.read()
        else:
            return None

    async def get_file(self, request):
        """Получить файл.
        Если файл не найден на текущем сервере, будет произведёт опрос соседних серверов.
        """
        file_name = request.match_info['file_name']
        try:
            return await self.get_local(request)
        except aiohttp.web.HTTPNotFound:
            content = await self._get_file_content_from_nodes(file_name)
            if not content:
                raise web.HTTPNotFound()
            if self.save_file:
                file_thread = Thread(target=self._save_file, args=(content, file_name,))
                file_thread.start()
            return self._construct_file_response(file_name=file_name, content=content)

    async def post_file(self, request):
        pass

    def _save_file(self, content, file_name):
        path = os.path.join(self.directory, file_name)
        with open(path, 'wb') as file:
            file.write(content)

    async def _get_file_content_from_nodes(self, file_name):
        """Опросить соседние сервера, получить содержимое файла от сеседей."""
        futures = [self.get_from_node(node=node, file_name=file_name) for node in self.nodes]
        done, pending = await asyncio.wait(futures)
        for done_task in done:
            response = done_task.result()
            if response['status_code'] == 200:
                return response['content']

        return None

    async def get_local(self, request):
        """Получить файл с текущего сервера."""
        file_name = request.match_info['file_name']
        content = await self._read_file(file_name)
        if not content:
            raise web.HTTPNotFound()

        return self._construct_file_response(file_name=file_name, content=content)

    @staticmethod
    def _construct_file_response(file_name, content):
        resp = web.Response(
            body=content,
            headers={
                'CONTENT-TYPE': 'application/octet-stream',
                'CONTENT-DISPOSITION': 'attachment; filename="{}"'.format(file_name)
            }
        )
        return resp

    @staticmethod
    async def get_from_node(node, file_name):
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get('http://{}/from_node/{}'.format(node, file_name)) as response:
                    content = await response.read()
                    status_code = response.status
                    return {'content': content, 'status_code': status_code}
            except aiohttp.client_exceptions.ClientConnectorError:
                return {'content': '404: Not Found', 'status_code': 404}


def parse_args():
    parser = argparse.ArgumentParser(description='This is a simple async-file-server')
    parser.add_argument(
        '--settings',
        type=str,
        action="store",
        dest="settings",
        help='Config file'
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    config_file = args.settings
    with open(config_file) as f:
        settings = yaml.load(f)

    server = StorageServer(
        host=settings['host'],
        port=settings['port'],
        directory=settings['directory'],
        nodes=settings['neighbors'],
        save_file=settings['save_file']
    )
    server.run()