import re
import json
import time
import inspect
from flask import Request
from functools import wraps
from ovs_extensions.api.exceptions import HttpForbiddenException, HttpNotAcceptableException, HttpNotFoundException,\
    HttpTooManyRequestsException, HttpUnauthorizedException, HttpUpgradeNeededException
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.dal.lists.storagerouterlist import StorageRouterList
from api_flask.response import ResponseOVS


def _find_request(args):
    """
    Finds the "request" object in args
    """
    for item in args:
        if isinstance(item, Request):
            return item


