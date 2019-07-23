import os

CELERY_BASE = 'celery'
CELERY_TASKS_LISTS_OUTPUT_PATH = os.path.join(os.path.sep, CELERY_BASE, 'tasks_list')