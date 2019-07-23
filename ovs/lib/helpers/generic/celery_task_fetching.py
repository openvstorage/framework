from ovs.celery_run import celery
from ovs.constants.celery import CELERY_TASKS_LISTS_OUTPUT_PATH
from ovs_extensions.constants.modules import BASE_OVS
from ovs.extensions.generic.configuration import Configuration

class TaskFetcher(object):

    @classmethod
    def fetch_celery(cls, to_md=False, filepath=None, editor_input=None):
        # type: (Optional[bool], Optional[str]) -> Dict[str, str]
        """
        This will call celery.tasks to list all currently implemented celerytasks, and return them as a dict. E.g.
            {celery_task_1: docstring of this task}
        :param to_md: will write to csv file if put on True. This csv output location will be located in the parameter filepath
        :param filepath: filepath to write csv file to. Will first check if a filepath is provided in the configmanagement under /celery/tasks_list
                                                        If no path is provided there, will write csv file to /tmp/celery_task_list.csv
        :return: Dict
        """
        filepath = filepath or Configuration.get(CELERY_TASKS_LISTS_OUTPUT_PATH, default='/tmp/celery_task_list.md')
        celery_tasks = cls._fetch_celery_tasks()
        if to_md:
            with open(filepath, 'w') as fh:
                fh.write(cls.dict_to_markdown(celery_tasks, editor_input=editor_input))
        return celery_tasks

    @classmethod
    def _fetch_celery_tasks(cls):
        # Celery does not know of our included modules just yet. So we import them.
        celery.loader.import_default_modules()
        return dict([(task, decorated_fun_task.__doc__) for task, decorated_fun_task in celery.tasks.iteritems() if task.startswith(BASE_OVS)])


    @classmethod
    def dict_to_markdown(cls, celery_tasks, editor_input=None):
        # type: (dict[str, str], Optional[dict[str, str]]) -> str
        """
        Input:
        {ovs.key1.name1: docstring,
         ovs.key2.name1: docstring,
         ovs.key2.name2: docstring}

        Output:
        ## Tasks
        ### Key1
        #### Name1
        ```
           docstring
        ```
        ### Key2
        #### Name1
        ```
           docstring
        ```
        #### Name2
        ```
            docstring
        ```

        :param celery_tasks: this dict contains a celery task name as key and the task docstring as value.
        :param editor_input: dict containing keys and values from celery task keys that need extra information. Will be shown with the full key at the bottom of the markdown file
        :return: formatted markdown version of the dict
        """
        # First build indented dict
        indented_dict = {}
        for celery_key, docstring in celery_tasks.iteritems():
            _, celery_function_folder, celery_function_name = celery_key.split('.')

            if celery_function_folder not in indented_dict:
                indented_dict[celery_function_folder] = {}

            indented_dict[celery_function_folder][celery_function_name] = docstring

        # Then build markdown layout
        out = '## Tasks\n'
        for folder, celery_functions in sorted(indented_dict.iteritems()):
            out += '### {0}\n'.format(folder.capitalize())
            for function_name, function_docstring in sorted(celery_functions.iteritems()):
                out += "#### {0}\n```{1}\n```\n".format(function_name, function_docstring)

        if editor_input:
            out += '## Editor input\n'
            for editor_key, editor_value in editor_input.iteritems():
                # To allign with the generated output, an additional tab is needed
                out += "### {0}\n```\n\t{1}\n```\n".format(editor_key, editor_value)
        return out

