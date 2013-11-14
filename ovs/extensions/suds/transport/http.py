# This program is free software; you can redistribute it and/or modify
# it under the terms of the (LGPL) GNU Lesser General Public License as
# published by the Free Software Foundation; either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Lesser General Public License for more details at
# ( http://www.gnu.org/licenses/lgpl.html ).
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
# written by: Jeff Ortel ( jortel@redhat.com )

"""
Contains classes for basic HTTP transport implementations.
"""

import urllib2
import base64
import bz2
import copy
import cStringIO
import gzip
import zlib
import socket
import sys
from urlparse import urlparse
from urllib import url2pathname
from cookielib import CookieJar
from logging import getLogger

from suds.transport import *
from suds.properties import Unskin, Definition


log = getLogger(__name__)


class HttpTransport(Transport):
    """
    HTTP transport using urllib2.  Provided basic http transport
    that provides for cookies, proxies but no authentication.
    """

    def __init__(self, **kwargs):
        """
        @param kwargs: Keyword arguments.
            - B{proxy} - An http proxy to be specified on requests.
                 The proxy is defined as {protocol:proxy,}
                    - type: I{dict}
                    - default: {}
            - B{timeout} - Set the url open timeout (seconds).
                    - type: I{float}
                    - default: 90
        """
        Transport.__init__(self)

        # Add options to the default set of options in case they do not exist
        Unskin(self.options).definitions['compression'] = Definition('compression', basestring, 'yes')
        Unskin(self.options).definitions['compmethods'] = Definition('compmethods', list, ['gzip', 'deflate', 'bzip2'])
        # Force the new options to take the default value supplied
        Unskin(self.options).prime()

        Unskin(self.options).update(kwargs)
        self.cookiejar = CookieJar()
        self.proxy = dict()
        self.urlopener = None

    # This implementation of "open" and "send"
    # could make it to the base class "Transport"

    def open(self, request):
        """
        Open a file or url
        If request.url can't be identified as a url, it will
        return the content in a file-like object
        @param request: A suds Request
        @type Request: suds.transport.Request
        @return: A file-like object
        @rtype: file
        """
        log.debug('opening: (%s)', request.url)

        fp = None
        location = request.url.lstrip()
        if location.startswith('<?'):
            log.debug('returning url (%s) as StringIO file')
            fp = cStringIO.StringIO(location)
        else:
            parsed = urlparse(request.url)
            if parsed.scheme == 'file':
                log.debug('opening file (%s) with open', parsed.path)
                try:
                    fp = open(url2pathname(parsed.path))
                except Exception, e:
                    raise TransportError(str(e), 500, None)
            else:
                log.debug('opening scheme (%s) over the network', parsed.scheme)
                fp = self.invoke(request, retfile=True)

        return fp

    def send(self, request):
        """
        Send a soap request
        @param request: A suds Request
        @type Request: suds.transport.Request
        @return: suds Reply
        @rtype: suds.transport.Reply
        """
        log.debug('sending: %s', request)
        return self.invoke(request)

    # for the base class Transport
    # this would be the definition of "invoke"
    # called by either open or send
    #
    # def invoke(self, request, retfile = False):
    #     raise NotImplementedError

    def invoke(self, request, retfile=False):
        """
        Open a connection.
        @param request: A suds Request
        @type Request: suds.transport.Request
        @param retfile: indicates if a file-like object is to be returned
        @type: bool 
        @return: A file-like object or a suds Reply
        @rtype: file or suds.transport.Reply
        """
        tm = self.options.timeout

        request = self.prerequest(request)
        u2request = urllib2.Request(request.url, request.message, request.headers)

        self.addcookies(u2request)

        request.headers = u2request.headers
        log.debug('request final headers:\n%s', request.headers)

        urlopener = self.u2opener()
        try:
            if self.u2ver() < 2.6:
                socket.settimeout(tm)
                u2response = urlopener.open(u2request)
            else:
                u2response = urlopener.open(u2request, timeout=tm)
        except urllib2.HTTPError, e:
            # This error is to mimic the original exception code
            if not retfile and e.code in (202, 204):
                result = None
            else:
                # use the same postreply() call to decode an error response
                body = e.read()
                reply = Reply(e.code, e.headers, body)
                reply = self.postreply(reply)
                memfile = cStringIO.StringIO(reply.message)
                raise TransportError(reply.message, e.code, memfile)

        # Updatecookies in the cookie jar
        self.getcookies(u2response, u2request)

        reply = Reply(200, u2response.headers.dict, u2response.read())
        reply = self.postreply(reply)
        log.debug('received reply:\n%s', reply)

        # Return what "open" is expecting ... a file-like object
        if retfile:
            reply = cStringIO.StringIO(reply.message)

        return reply
            
    # I would personally remove this function
    # it is a one-liner that would substitute a one liner
    
    def addcookies(self, u2request):
        """
        Add cookies in the cookiejar to the request.
        @param u2request: A urllib2 request.
        @rtype: u2request: urllib2.Requet.
        """
        self.cookiejar.add_cookie_header(u2request)
        
    # I would personally remove this function
    # it is a one-liner that would substitute a one liner
    def getcookies(self, u2response, u2request):
        """
        Add cookies in the request to the cookiejar.
        @param u2request: A urllib2 request.
        @rtype: u2request: urllib2.Requet.
        """
        self.cookiejar.extract_cookies(u2response, u2request)
        
    # I think there was a bug in the original code since self.urlopener
    # was never assigned a value and the code kept on creating a new
    # opener

    # Of course if self.urlopener is None or self.options.proxy have changed
    # a new urlopener has to be created and stored
    def u2opener(self):
        """
        Create a urllib opener.
        @return: An opener.
        @rtype: I{OpenerDirector}
        """
        if self.urlopener == None or self.proxy != self.options.proxy:
            self.urlopener = urllib2.build_opener(*self.u2handlers())

        return self.urlopener
        
    # Make a copy (if needed) of the options.proxy to detect changes
    # during runtime
    def u2handlers(self):
        """
        Get a collection of urllib handlers.
        @return: A list of handlers to be installed in the opener.
        @rtype: [Handler,...]
        """
        handlers = []
        self.proxy = copy.copy(self.options.proxy)
        handlers.append(urllib2.ProxyHandler(self.proxy))
        return handlers

    def u2ver(self):
        """
        Get the major/minor version of the urllib2 lib.
        @return: The urllib2 version.
        @rtype: float
        """
        try:
            part = urllib2.__version__.split('.', 1)
            n = float('.'.join(part))
            return n
        except Exception, e:
            log.exception(e)
            return 0

    def __deepcopy__(self, memo={}):
        clone = self.__class__()
        p = Unskin(self.options)
        cp = Unskin(clone.options)
        cp.update(p)
        return clone

    # This function pre-processes the request before sending it
    # In my opinion, the base class Transport should define it as an empty stub
    # or simply with "return request"

    # The HttpAuthenticated (below) is a perfect example, because instead of
    # redifining "open" and "send" to add the credentials it would only
    # redefine "preprequest" and then call the baseclass.prequest

    def prerequest(self, request):

        if self.options.compression == 'yes':
            compmethods = ','.join(self.options.compmethods)
            request.headers['Accept-Encoding'] = compmethods
            log.debug('requesting the following compressions: %s', compmethods)

        return request

    # This function post-processes the reply after receiving it (obvious!)
    # In my opinion, the base class Transport should define it as an empty stub
    # or simply with "return reply"

    def postreply(self, reply):

        if self.options.compression in ['yes', 'auto']:
            for header, headerval in reply.headers.items():   # this needs to be items() and not iteritems() because it's sometimes an httplib.HTTPMessage (when decoding an error response) and that doesn't support iteritems()!
                if header.lower() == 'content-encoding':
                    log.debug('http reply with a content-encoding header')
                    if headerval == 'gzip':
                        log.debug('decompressing gzip content')
                        replydatafile = cStringIO.StringIO(reply.message)
                        gzipper = gzip.GzipFile(fileobj=replydatafile)
                        reply.message = gzipper.read()
                    elif headerval == 'deflate':
                        # decompress the deflate content
                        log.debug('decompressing deflate content')
                        try:
                            reply.message = zlib.decompress(reply.message)
                        except zlib.error:
                            # Many web sites fail to send the first bytes of the header
                            reply.message = zlib.decompress(reply.message, -zlib.MAX_WBITS)
                    elif headerval == 'bzip2':
                        # decompress bzip content
                        log.debug('decompressing unix compress content')
                        reply.message = bz2.decompress(reply.message)
                        pass
                    else:
                        # unknown scheme
                        log.debug('unsupported content-encoding scheme')
                        pass

                    break

        return reply


class HttpAuthenticated(HttpTransport):
    """
    Provides basic http authentication for servers that don't follow
    the specified challenge / response model.  This implementation
    appends the I{Authorization} http header with base64 encoded
    credentials on every http request.
    """

    def open(self, request):
        self.addcredentials(request)
        return HttpTransport.open(self, request)

    def send(self, request):
        self.addcredentials(request)
        return HttpTransport.send(self, request)

    def addcredentials(self, request):
        credentials = self.credentials()
        if not (None in credentials):
            # Bytes and strings are different in Python 3 than in Python 2.x
            if sys.version_info < (3,0):
                encoded = base64.encodestring(':'.join(credentials))
                # There is an extra terminal \n in Python 2.x
                basic = 'Basic %s' % encoded[:-1]
            else:
                encoded = base64.urlsafe_b64encode(':'.join(credentials).encode())
                # The encoded string is a byte string and so is prefixed with b
                # To eliminate the prefix, we convert it back to str type
                basic = 'Basic %s' % encoded.decode()
            request.headers['Authorization'] = basic

    def credentials(self):
        return (self.options.username, self.options.password)
