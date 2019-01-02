import os
# All CLI commands should output logging to the file to avoid cluttering
os.environ['OVS_LOGTYPE_OVERRIDE'] = 'file'

import click
import subprocess
from .setup import setup_group
from .config import config_group
from .misc.misc import rollback, update, collect_logs
from .remove import remove_group
from .monitor import monitor_group
from .services import start_group, stop_group
from ovs_extensions.cli.cli import unittest
from ovs_extensions.constants.scripts import BASE_SCRIPTS
from IPython import embed


"""
Not something that we're proud of but it has to be this way :(
The unittest do not require any implementation to run, everything gets mocked
However when loading in all other commands, the imports might/do fetch instances of real implementation
Which don't do anything or cannot be instantiated
Thus we have to import controllers whenever we invoke a command :(
"""


class AddonScript(click.Command):

    def __init__(self, script_path, name, *args, **kwargs):
        """
        Initialize an addon script
        :param script_path: Path to the addon script
        :type script_path: str
        :param name: Name of the addon script
        :type name: str
        """
        self.script_path = script_path
        help = 'Run the command for the {} addon'.format(name)
        super(AddonScript, self).__init__(callback=self.script_callback, name=name, help=help, *args, **kwargs)

    def parse_args(self, ctx, args):
        # type: (click.Context, list) -> list
        """
        Override the parse_args. We only care about the string values given to the addon command
        It'll decide what to do with the arguments itself
        This also disables click retrieving the help. Itll pass it along to the addon
        :param ctx: Context
        :type ctx: click.Context
        :param args: Supplied args
        :type args: list
        :return: List of args
        :rtype: list
        """
        ctx.args = args
        return args

    def script_callback(self, *args, **kwargs):
        # type: (*any, **any) -> None
        """
        Callback that invokes the passed on script
        :return: None
        """
        _ = kwargs
        print subprocess.check_output([self.script_path] + list(args))

    def invoke(self, ctx):
        # type: (click.Context) -> any
        """
        Overrule the invoke. This click command does not process any arguments and just passes them on
        :param ctx: Context given
        :type ctx: click.Context
        :return: Output of the addon command
        :rtype: any
        """
        if self.callback is not None:
            return ctx.invoke(self.callback, *ctx.args)


class CLI(click.Group):
    """
    Click CLI which will dynamically loads all addon commands
    Implementations require an entry point
    An entry point is defined as:
    @click.group(cls=CLI)
    def entry_point():
        pass
    if __name__ == '__main__':
        entry_point()
    """

    def __init__(self, *args, **kwargs):
        # type: (*any, **any) -> None
        super(CLI, self).__init__(*args, **kwargs)

    def list_commands(self, ctx):
        # type: (click.Context) ->List[str]
        """
        Lists all possible commands found within the directory of this file
        All modules are retrieved
        :param ctx: Passed context
        :return: List of files to look for commands
        :rtype: List[str]
        """
        _ = ctx
        non_dynamic = self.commands.keys()
        sub_commands = self._discover_methods().keys()  # Returns all underlying modules
        total_commands = non_dynamic + sub_commands
        total_commands.sort()
        return total_commands

    def get_command(self, ctx, name):
        # type: (click.Context, str) -> callable
        """
        Retrieves a command to execute
        :param ctx: Passed context
        :param name: Name of the command
        :return: Function pointer to the command or None when no import could happen
        :rtype: callable
        """
        cmd = self.commands.get(name)
        if cmd:
            return cmd
        # More extensive - build the command and register
        discovery_data = self._discover_methods()
        if name in discovery_data.keys():
            script_path = discovery_data[name]
            # The current passed name is a module. Wrap it up in a group and add all commands under it dynamically
            command = AddonScript(script_path=script_path, name=name)
            self.add_command(command)
            return command

    @classmethod
    def _discover_methods(cls):
        # type: () -> Dict[str, str]
        """
        Discovers all possible scripts within the BASE_SCRIPTS folder
        :return: Dict with the filename as key and the path as value
        :rtype: Dict[str, str]
        """
        discovered = {}
        for name in os.listdir(BASE_SCRIPTS):
            full_path = os.path.join(BASE_SCRIPTS, name)
            if os.path.isfile(full_path) and name.endswith('.sh'):
                discovered[name.rstrip('.sh')] = full_path
        return discovered

    def format_commands(self, ctx, formatter):
        """
        Extra format methods for multi methods that adds all the commands after the options.
        Overruled to add Addon commands as a separate list
        """
        command_rows = []
        addon_rows = []
        for subcommand in self.list_commands(ctx):
            cmd = self.get_command(ctx, subcommand)
            # What is this, the tool lied about a command. Ignore it
            if cmd is None:
                continue

            help = cmd.short_help or ''
            if isinstance(cmd, AddonScript):
                addon_rows.append((subcommand, help))
            else:
                command_rows.append((subcommand, help))

        for row_section, rows in [('Commands', command_rows), ('Addons', addon_rows)]:
            if rows:
                with formatter.section(row_section):
                    formatter.write_dl(rows)

    def format_usage(self, ctx, formatter):
        """
        Writes the usage line into the formatter.
        Overruled to state 'ovs' instead of 'entry.py'
        """
        pieces = self.collect_usage_pieces(ctx)
        formatter.write_usage(self.name, ' '.join(pieces))


@click.group(name='ovs', help='Open the OVS python shell or run an ovs command', invoke_without_command=True, cls=CLI)
@click.pass_context
def ovs(ctx):
    if ctx.invoked_subcommand is None:
        embed()
    # Documentation purposes:
    # else:
    # Do nothing: invoke subcommand


groups = [setup_group, config_group, rollback, update, remove_group, monitor_group, unittest, start_group, stop_group, collect_logs]
for group in groups:
    ovs.add_command(group)
