import os
import ConfigParser

parser = ConfigParser.RawConfigParser()
parser.read(os.path.dirname(__file__) + '/config/settings.cfg')

DEBUG = True
TEMPLATE_DEBUG = DEBUG

BASE_NAME = parser.get('main', 'base_name')
BASE_WWW_DIR = os.path.dirname(__file__)
BASE_LOG_DIR = parser.get('main', 'log_folder') + '/' + BASE_NAME

FRONTEND_ROOT = '/' + BASE_NAME
STATIC_URL    = '/' + BASE_NAME + '/static/'  # STATIC_URL must end with a slash

FORCE_SCRIPT_NAME = FRONTEND_ROOT

ADMINS = (
    ('Kenneth Henderick', 'kenneth.henderick@cloudfounders.com'),
)

MANAGERS = ADMINS

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_WWW_DIR + '/' + BASE_NAME + '.db.sqlite3'
    }
}

ALLOWED_HOSTS = []
TIME_ZONE = 'Europe/Brussels'
LANGUAGE_CODE = 'en-us'
LOGIN_URL = BASE_NAME + '.frontend.login_view'

SITE_ID = 1
USE_I18N = True
USE_L10N = True
USE_TZ = True
MEDIA_ROOT = ''
MEDIA_URL = ''

STATIC_ROOT = ''
STATICFILES_DIRS = (
)
STATICFILES_FINDERS = (
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder'
)

SECRET_KEY = '6e-%9ce4rx125jw69vp-ar1fpu9ta5#g6w73^vvrf65+14ovpi'

TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.Loader',
    'django.template.loaders.app_directories.Loader'
)

MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware'
)

from django.conf.global_settings import TEMPLATE_CONTEXT_PROCESSORS
TEMPLATE_CONTEXT_PROCESSORS += (
    'django.core.context_processors.request',
)

ROOT_URLCONF = BASE_NAME + '.urls'

# Python dotted path to the WSGI application used by Django's runserver.
WSGI_APPLICATION = 'api.wsgi.application'

TEMPLATE_DIRS = (
    BASE_WWW_DIR + '/templates',
)

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.sites',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
)

CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.memcached.MemcachedCache',
        'LOCATION': '10.100.138.253:11211',
    }
}

SESSION_SERIALIZER = 'django.contrib.sessions.serializers.JSONSerializer'
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'simple': {
            'format': '%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s',
        },
    },
    'handlers': {
        'logfile': {
            'class': 'logging.handlers.WatchedFileHandler',
            'filename': BASE_LOG_DIR + '/django.log',
            'formatter': 'simple',
        },
        'accessfile': {
            'class': 'logging.handlers.WatchedFileHandler',
            'filename': BASE_LOG_DIR + '/access.log',
            'formatter': 'simple',
        },
    },
    'loggers': {
        'django': {
            'handlers': ['logfile'],
            'level': 'ERROR',
            'propagate': False,
        },
        BASE_NAME: {
            'handlers': ['logfile'],
            'level': 'INFO',
            'propogate': False
        },
        'access': {
            'handlers': ['accessfile'],
            'level': 'INFO',
            'propagate': False,
        },
    },
}
