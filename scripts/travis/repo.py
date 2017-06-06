#!/usr/bin/env python
import os
branch = os.environ.get('TRAVIS_BRANCH')
mapping = {'master': 'unstable',
           'develop': 'fwk-develop'}
if branch not in mapping:
    branch = 'develop'
print mapping.get(branch)
