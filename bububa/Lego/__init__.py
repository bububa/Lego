#!/usr/bin/env python
# encoding: utf-8
__version__ = '0.0.1'
__creater__ = 'prof.syd.xu@gmail.com'

try:
    __import__('pkg_resources').declare_namespace(__name__)
except ImportError:
    from pkgutil import extend_path
    __path__ = extend_path(__path__, __name__)

if __name__ == '__main__': print __version__