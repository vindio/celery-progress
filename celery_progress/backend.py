from abc import ABCMeta, abstractmethod
from decimal import Decimal

from celery.result import AsyncResult, allow_join_result


PROGRESS_STATE = 'PROGRESS'


class AbstractProgressRecorder():
    __metaclass__ = ABCMeta

    @abstractmethod
    def set_progress(self, current, total, description="", progress_id=None):
        pass

    @abstractmethod
    def stop_task(self, current, total, exc):
        pass


class ConsoleProgressRecorder(AbstractProgressRecorder):

    def set_progress(self, current, total, description="", progress_id=None):
        id_info = f' (id={progress_id})' if progress_id is not None else ''
        print(f'processed {current} items of {total}. {description}{id_info}')

    def stop_task(self, current, total, exc):
        pass


class ProgressRecorder(AbstractProgressRecorder):

    def __init__(self, task):
        self.task = task

    def set_progress(self, current, total, description="", progress_id=None):
        percent: float = 0.0
        if total > 0:
            percent = round(
                float((Decimal(current) / Decimal(total)) * Decimal(100)), 2
            )
        meta = {
            'current': current,
            'total': total,
            'percent': percent,
            'description': description
        }
        if progress_id is not None:
            meta['progress_id'] = progress_id
        self.task.update_state(
            state=PROGRESS_STATE,
            meta=meta
        )

    def stop_task(self, current, total, exc):
        self.task.update_state(
            state='FAILURE',
            meta={
                'current': current,
                'total': total,
                'percent': 100.0,
                'exc_message': str(exc),
                'exc_type': str(type(exc))
            }
        )


class Progress(object):

    def __init__(self, task_id):
        self.task_id = task_id
        self.result = AsyncResult(task_id)

    def get_info(self):
        if self.result.ready():
            success = self.result.successful()
            with allow_join_result():
                result = self.result.get(self.task_id) if success \
                        else str(self.result.info)
                return {
                    'task_id': self.task_id,
                    'complete': True,
                    'success': success,
                    'progress': _get_completed_progress(),
                    'result': result
                }
        elif self.result.state == PROGRESS_STATE:
            return {
                'task_id': self.task_id,
                'complete': False,
                'success': None,
                'progress': self.result.info,
            }
        elif self.result.state in ['PENDING', 'STARTED']:
            return {
                'task_id': self.task_id,
                'complete': False,
                'success': None,
                'progress': _get_unknown_progress(),
            }
        return self.result.info


def _get_completed_progress():
    return {
        'current': 100,
        'total': 100,
        'percent': 100,
    }


def _get_unknown_progress():
    return {
        'current': 0,
        'total': 100,
        'percent': 0,
    }
