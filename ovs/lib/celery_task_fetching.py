from ovs.celery_run import celery
from ovs.constants.celery import CELERY_TASKS_LISTS_OUTPUT_PATH
from ovs_extensions.constants.modules import BASE_OVS
from ovs.extensions.generic.configuration import Configuration

class TaskFetcher(object):

    @classmethod
    def fetch_celery(cls, to_csv=False, filepath=None):
        # type: (None, Optional[bool], Optional[str]) -> Dict[str, str]
        """
        This will call celery.tasks to list all currently implemented celerytasks, and return them as a dict. E.g.
            {celery_task_1: docstring of this task}
        :param to_csv: will write to csv file if put on True. This csv output location will be located in the parameter filepath
        :param filepath: filepath to write csv file to. Will first check if a filepath is provided in the configmanagement under /celery/tasks_list
                                                        If no path is provided there, will write csv file to /tmp/celery_task_list.csv
        :return: Dict
        """
        filepath = filepath or Configuration.get(CELERY_TASKS_LISTS_OUTPUT_PATH, default='/tmp/celery_task_list.md')
        celery_tasks = cls._fetch_celery_tasks()
        if to_csv:
            with open(filepath, 'w') as fh:
                fh.write(cls.dict_to_markdown(celery_tasks))
        return celery_tasks

    @classmethod
    def _fetch_celery_tasks(cls):
        # Celery does not know of our included modules just yet. So we import them.
        celery.loader.import_default_modules()
        return dict([(task, decorated_fun_task.__doc__) for task, decorated_fun_task in celery.tasks.iteritems() if task.startswith(BASE_OVS)])

    @classmethod
    def _format_to_markdown(cls, title, body):
        body = body.replace('\t', '\n')
        return "### {0}\n"\
               "```{1}```\n".format(title, body)

    @classmethod
    def dict_to_markdown(cls, d):
        return '\n'.join([cls._format_to_markdown(title, body) for title, body in sorted(d.iteritems())])

