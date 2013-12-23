"""
Dependency injection module

This module init will execute all required code to startup injection
into all required providers. For now, the frameworks are hard-coded,
but they can be read from a configuration file in a later stage when/if this
would be appropriate
"""

from ovs.plugin.injection.injector import Injector

from ovs.plugin.provider.configuration import Configuration
from ovs.plugin.provider.tools import Tools
from ovs.plugin.provider.package import Package
from ovs.plugin.provider.service import Service
from ovs.plugin.provider.logger import Logger
from ovs.plugin.provider.process import Process

Injector.inject('jumpscale', [Configuration, Tools, Package, Service, Logger, Process])
