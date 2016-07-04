# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

import json


class HttpException(Exception):
    def __init__(self, status, error, error_description):
        self.status_code = status
        self.error = error
        self.error_description = error_description
        self.data = json.dumps({'error': error,
                                'error_description': error_description})


class HttpBadRequestException(HttpException):
    def __init__(self, error, error_description):
        super(HttpBadRequestException, self).__init__(400, error, error_description)


class HttpUnauthorizedException(HttpException):
    def __init__(self, error, error_description):
        super(HttpUnauthorizedException, self).__init__(401, error, error_description)


class HttpForbiddenException(HttpException):
    def __init__(self, error, error_description):
        super(HttpForbiddenException, self).__init__(403, error, error_description)


class HttpNotFoundException(HttpException):
    def __init__(self, error, error_description):
        super(HttpNotFoundException, self).__init__(404, error, error_description)


class HttpMethodNotAllowedException(HttpException):
    def __init__(self, error, error_description):
        super(HttpMethodNotAllowedException, self).__init__(405, error, error_description)


class HttpNotAcceptableException(HttpException):
    def __init__(self, error, error_description):
        super(HttpNotAcceptableException, self).__init__(406, error, error_description)


class HttpGoneException(HttpException):
    def __init__(self, error, error_description):
        super(HttpGoneException, self).__init__(410, error, error_description)


class HttpTooManyRequestsException(HttpException):
    def __init__(self, error, error_description):
        super(HttpTooManyRequestsException, self).__init__(429, error, error_description)


class HttpInternalServerErrorException(HttpException):
    def __init__(self, error, error_description):
        super(HttpInternalServerErrorException, self).__init__(500, error, error_description)

