import json


from channels.generic.websocket import (
    AsyncWebsocketConsumer,
    AsyncJsonWebsocketConsumer
)


from celery_progress.backend import Progress


import logging


logger = logging.getLogger(__name__)


class ProgressError(Exception):
    def __init__(self, message=None):
        self.message = message
        super().__init__(message)

    def __str__(self):
        return self.message


class RequestRequired(ProgressError):
    def __init__(self):
        super().__init__('type is required')


class UnknownRequest(ProgressError):
    def __init__(self):
        super().__init__('unknown request type')


class TaskIdRequired(ProgressError):
    def __init__(self):
        super().__init__('task_id is required')


class TaskIdInvalid(ProgressError):
    def __init__(self, task_id=''):
        super().__init__(f'task_id is not valid {task_id}')


class TaskIdsRequired(ProgressError):
    def __init__(self):
        super().__init__('task_ids is required')


class ProgressConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.task_id = self.scope['url_route']['kwargs']['task_id']

        await self.channel_layer.group_add(
            self.task_id,
            self.channel_name
        )

        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(
            self.task_id,
            self.channel_name
        )

    async def receive(self, text_data):
        text_data_json = json.loads(text_data)
        task_type = text_data_json['type']

        if task_type == 'check_task_completion':
            await self.channel_layer.group_send(
                self.task_id,
                {
                    'type': 'update_task_progress',
                    'data': {**Progress(self.task_id).get_info()}
                }
            )

    async def update_task_progress(self, event):
        data = event['data']

        await self.send(text_data=json.dumps(data))


class ProgressMultiTaskConsumer(AsyncJsonWebsocketConsumer):
    async def connect(self):
        self.tasks = set()
        await self.accept()
        logging.info(
            'New client connected: %s',
            self.scope.get('user', 'UNKNOWN USER')
        )

    async def disconnect(self, close_code):
        logging.info('Disconecting client. tasks=%s', self.tasks)
        try:
            await self.unfollow_tasks(self.tasks)
        except ProgressError:
            pass

    async def receive_json(self, content):
        try:
            request = content.get('type', None)
            task_id = content.get('task_id', None)
            task_ids = set(content.get('task_ids', []))
            logging.info('Got request: %s', content)
            if request is None:
                raise RequestRequired()
            if request == 'check_task_completion':
                await self.check_task_completion(task_id)
            elif request == 'follow_tasks':
                await self.follow_tasks(task_ids)
            elif request == 'follow_task':
                await self.follow_task(task_id)
            elif request == 'unfollow_task':
                await self.unfollow_task(task_id)
            elif request == 'unfollow_tasks':
                await self.follow_tasks(task_ids)
            else:
                raise UnknownRequest()
        except ProgressError as error:
            logger.error(error.message)
            await self.send_json({'error': error.message})
        except Exception as error:
            logger.error(str(error))
            await self.send_json({'error': f'FATAL ERROR: {error}'})

    async def check_task_completion(self, task_id):
        if task_id is None:
            raise TaskIdRequired()
        await self.channel_layer.group_send(
            task_id,
            {
                'type': 'update_task_progress',
                'data': {**Progress(task_id).get_info()}
            }
        )

    async def follow_task(self, task_id):
        if task_id is None:
            raise TaskIdRequired()
        self.tasks.add(task_id)
        await self.channel_layer.group_add(
            task_id,
            self.channel_name
        )

    async def unfollow_task(self, task_id):
        if task_id is None:
            raise TaskIdRequired()
        try:
            self.tasks.remove(task_id)
        except KeyError:
            raise TaskIdInvalid(task_id)
        await self.channel_layer.group_discard(
            task_id,
            self.channel_name
        )

    async def follow_tasks(self, task_ids):
        logging.info('Start Following tasks: %s', [t for t in task_ids])
        if task_ids is None or len(task_ids) == 0:
            raise TaskIdsRequired()
        for task_id in task_ids:
            await self.follow_task(task_id)

    async def unfollow_tasks(self, task_ids):
        task_ids_copy = list(task_ids)
        if task_ids is None or len(task_ids) == 0:
            raise TaskIdsRequired()
        for task_id in task_ids_copy:
            await self.unfollow_task(task_id)
        logging.info('Unfollowed tasks: %s', task_ids_copy)

    async def update_task_progress(self, event):
        data = event['data']
        logging.debug('Updating task progrress: %s', event)
        await self.send_json(data)
