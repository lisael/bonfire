'''
Created on 05.03.15

@author = mharder
'''

from __future__ import division, print_function

import configparser
import os
from string import Template

import keyring
import arrow


def get_config():
    config = configparser.ConfigParser()
    config.read(['bonfire.cfg', os.path.expanduser('~/.bonfire.cfg')])

    return config, cfg_to_dict(config)


def cfg_to_dict(cfg):
    dictcfg = {k: dict(v) for k, v in dict(cfg).items()}
    toplevel = dictcfg.setdefault("default", {})
    dictcfg.update(toplevel)
    return dictcfg


def get_templated_option(cfg, section, option, kwargs={}):
    template = Template(cfg.get(section, option))

    dt = arrow.now('local')

    mapping = {
        "today": dt.format("YYYY-MM-DD"),
        "time": dt.format("HH:mm:ss"),
        "now": dt.format("YYYY-MM-DD HH:mm:ss.SS")
    }

    return template.substitute(mapping, **kwargs)


def store_password_in_keyring(host, username, password):
    keyring.set_password('bonfire_' + host, username, password)


def get_password_from_keyring(host, username):
    return keyring.get_password('bonfire_' + host, username)
