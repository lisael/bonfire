#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division, print_function, absolute_import

# Todo:

import logging
import warnings
from bonfire import __version__

__author__ = "Malte Harder"
__copyright__ = "Blue Yonder"
__license__ = "new-bsd"

_logger = logging.getLogger(__name__)

from datetime import timedelta
import sys

import click
import getpass
import arrow

from .config import get_config, get_password_from_keyring, store_password_in_keyring, get_templated_option
from .graylog_api import GraylogAPI, SearchRange, SearchQuery, TermQuery
from .output import run_logprint
from .formats import tail_format, dump_format


def cli_error(msg):
    click.echo(click.style(msg, fg='red'))
    sys.exit(1)


@click.command()
@click.option("--node", default=None, help="Label of a preconfigured graylog node")
@click.option("-h", "--host", default=None, help="Your graylog node's host")
@click.option("--tls", default=False, is_flag=True, help="Uses TLS")
@click.option("--port", default=12900, help="Your graylog port (default: 12900)")
@click.option("--endpoint", default="/", help="Your graylog API endpoint e.g /api (default: /)")
@click.option("-u", "--username", default=None, help="Your graylog username")
@click.option("-p", "--password", default=None, help="Your graylog password (default: prompt)")
@click.option("-k/-nk", "--keyring/--no-keyring", default=False, help="Use keyring to store/retrieve password")
@click.option("-@", "--search-from", default=None, help="Query range from")
@click.option("-#", "--search-to", default=None, help="Query range to (default: now)")
@click.option('-t', '--tail', 'mode', flag_value='tail', default=True, help="Show the last --limit lines for the query (default)")
@click.option('-d', '--dump', 'mode', flag_value='dump', help="Print the query result as a csv")
@click.option("-l", "--value-list", 'mode', flag_value="val_list", help="List unique values of the given --field")
@click.option("-f", "--follow", default=False, is_flag=True, help="Poll the logging server for new logs matching the query (sets search from to 10 minutes ago, limit to None)")
@click.option("-i", "--interval", default=1000, help="Polling interval in ms (default: 1000)")
@click.option("-n", "--limit", default=10, help="Limit the number of results (default: 10)")
@click.option("-a", "--latency", default=2, help="Latency of polling queries (default: 2)")
@click.option("-r", "--stream", default=None, help="Stream ID of the stream to query (default: no stream filter)")
@click.option('--field', '-e', multiple=True, help="Fields to include in the query result", default=["message", "source", "facility", "line", "module"])
@click.option('--template-option', '-x', multiple=True, help="Template options for the stored query")
@click.option('--sort', '-s', default=None, help="Field used for sorting (default: timestamp)")
@click.option("--asc/--desc", default=False, help="Sort ascending / descending")
@click.option("--proxy", default=None, help="Proxy to use for the http/s request")
@click.option("-q", "--query", default="*")
@click.argument("more_query", nargs=-1)
def run(host,
        node,
        port,
        endpoint,
        tls,
        username,
        password,
        keyring,
        search_from,
        search_to,
        mode,
        follow,
        interval,
        limit,
        latency,
        stream,
        field,
        template_option,
        sort,
        asc,
        proxy,
        query,
        more_query):
    """
    Bonfire - A graylog CLI client
    """
    cfg, dict_cfg = get_config()

    def get_nodecfg(node=None, nodecfg=None):
        if node:
            nodecfg = dict_cfg[f"node:{node}"]
        if username is None and nodecfg["username"] is None:
            nodecfg["username"] = click.prompt(
                "Enter username for {host}:{port}".format(**nodecfg),
                default=getpass.getuser())
        else:
            nodecfg.setdefault("username", username)
        use_keyring = dict_cfg.get("use_keyring", keyring)
        if use_keyring and password is None:
            nodecfg["password"] = get_password_from_keyring(nodecfg["host"],
                                                            nodecfg["username"])
        if nodecfg["password"] is None:
            nodecfg["password"] = click.prompt(
                "Enter password for {username}@{host}:{port}".format(
                    **nodecfg), hide_input=True)
        if use_keyring:
            store_password_in_keyring(nodecfg["host"], nodecfg["username"],
                                      nodecfg["password"])
        return nodecfg

    # Configure the graylog API object
    if node is not None:
        # The user specified a preconfigured node, take the config from there
        nodecfg = get_nodecfg()
    else:
        if host is not None:
            # A manual host configuration is used
            scheme = "https" if tls else "http"
            nodecfg = dict(scheme=scheme,
                           proxies={scheme: proxy} if proxy else None,
                           host=host, port=port, endpoint=endpoint,
                           username=username )
            nodecfg = get_nodecfg(None, nodecfg)
        else:
            if "node:default" in dict_cfg:
                nodecfg = get_nodecfg("default")
            else:
                cli_error("Error: No host or node configuration specified and no default found.")

    gl_api = GraylogAPI(**nodecfg)

    # Check if the query should be retrieved from the configuration
    query = query.split() + list(more_query)

    if query[0][0] == ":":
        section_name = "query" + query[0]
        template_options = dict(map(lambda t: tuple(str(t).split("=", 1)), template_option))
        if mode == "val_list":
            # TODO: warn that cli provided query is ignored
            query = ["*"]
        else:
            if cfg.has_option(section_name, "query"):
                cfg_query = get_templated_option(cfg, section_name, "query", template_options)
            else:
                cfg_query = "*"
            if len(query) > 1:
                query = " ".join([cfg_query, "AND"] + list(query[1:]))
            else:
                query = cfg_query

        if mode != "val_list":
            if cfg.has_option(section_name, "limit"):
                limit = get_templated_option(cfg, section_name, "limit", template_options)

            if cfg.has_option(section_name, "from"):
                search_from = get_templated_option(cfg, section_name, "from", template_options)

            if cfg.has_option(section_name, "to"):
                search_to = get_templated_option(cfg, section_name, "to", template_options)

            if cfg.has_option(section_name, "sort"):
                sort = get_templated_option(cfg, section_name, "sort", template_options)

            if cfg.has_option(section_name, "asc"):
                asc = get_templated_option(cfg, section_name, "asc", template_options)

            if cfg.has_option(section_name, "fields"):
                field = get_templated_option(cfg, section_name, "fields", template_options).split(",")

        if cfg.has_option(section_name, "stream"):
            stream = get_templated_option(cfg, section_name, "stream", template_options)
    else:
        query = " ".join(query)

    # Configure the base query
    if search_from is None:
        # by default search values form the last 24 hours
        if mode == "val_list":
            search_from = arrow.now("local") - timedelta(days=1)
            search_to = "now"
            relative = True
        else:
            search_from = "5 minutes ago"
            relative = False

    sr = SearchRange(from_time=search_from, to_time=search_to, relative=relative)

    # Pass none if the list of fields is empty
    fields = None
    if field:
        fields = list(field)

    if limit <= 0:
        limit = None

    # Set limit to None, sort to none and start time to 10 min ago, if follow
    # is active
    if follow:
        if mode == "val_list":
            warnings.warn("No point to follow in list value mode")
        else:
            limit = None
            sort = None
            sr.from_time = arrow.now('local').replace(seconds=-latency - 10)
            sr.to_time = arrow.now('local').replace(seconds=-latency)

    # Get the user permissions
    userinfo = gl_api.user_info(nodecfg["username"])

    # If the permissions are not set or a stream is specified
    stream_filter = None
    streams = gl_api.streams()["streams"]
    if stream:
        for s in streams:
            if s['id'] == stream:
                break
        else:
            for s in streams:
                if s['title'] == stream:
                    stream = s['id']
                    break
            else:
                cli_error("Stream %s not found on server" % stream)
        stream_filter = "streams:{}".format(stream)
    elif userinfo["permissions"] != ["*"] and gl_api.default_stream is None:
        click.echo("Please select a stream to query:")
        for i, stream in enumerate(streams):
            click.echo("{}: Stream '{}' (id: {})".format(i, stream["title"], stream["id"]))
        i = click.prompt("Enter stream number:", type=int, default=0)
        stream = streams[i]["id"]
        stream_filter = "streams:{}".format(stream)

    if mode == "val_list":
        q = TermQuery(search_range=sr, query="*", filter=stream_filter, field=field[0])
    else:
        q = SearchQuery(search_range=sr, query=query, limit=limit,
                        filter=stream_filter, fields=fields, sort=sort,
                        ascending=asc)

    # Check the mode in which the program should run
    if mode == "val_list":
        # TODO: warn that cli provided query is ignored
        result = gl_api.terms(q)
        print(";".join(result["terms"].keys()))
    else:
        if mode == "tail":
            formatter = tail_format(fields)
        elif mode == "dump":
            formatter = dump_format(fields)

        run_logprint(gl_api, q, formatter, follow, interval, latency)


if __name__ == "__main__":
    run()
