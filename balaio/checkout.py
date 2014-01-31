#coding: utf-8
import os
import time
from datetime import datetime
from StringIO import StringIO
from multiprocessing.dummy import Pool as ThreadPool

import transaction
import scieloapi

import utils
import models
import meta_extractor
from checkin import PackageAnalyzer
from uploader import StaticScieloBackend


FILES_EXTENSION = ['xml', 'pdf',]
IMAGES_EXTENSION = ['tif', 'eps']


class CheckoutList(list):
    """
    Child class adapted to have a specific behavior in append method.
    """

    def append(self, item):
        """
        Add the param item on a parent list if is a expected type

        :param item: Any Attempt like object
        """
        if isinstance(item, models.Attempt):
            item.queued_checkout=True

        super(CheckoutList, self).append(item)


def get_static_files(attempt, ext):
    """
    Get the static files from the zip file
    Return a generator to a specific extension
    """

    pkg_analyzer = PackageAnalyzer(attempt.filepath)

    return pkg_analyzer.get_fps(ext)


def target_path(aid, arq_name):
    """
    Produces the path to the static file based on file name and aid
    :param aid: Article ID
    :param arq_name: Name of file extracted from the zip file
    """
    return '/articles/%s/%s' % (aid, os.path.basename(arq_name))


def upload_static_files(attempt, cfg):
    """
    Send the ``PDF``, ``XML`` files to the static server

    :param attempt: Attempt object
    :param cfg: configuration file
    """
    uri_dict = {}

    with StaticScieloBackend(cfg.get('static_server', 'username'),
                             cfg.get('static_server', 'password'),
                             cfg.get('static_server', 'path'),
                             cfg.get('static_server', 'host')) as static:

        for ext in FILES_EXTENSION:
            for stc in get_static_files(attempt, ext):
                uri = static.send(StringIO(stc.read()),
                                  target_path(attempt.articlepkg.aid, stc.name))

                uri_dict[ext] = uri

        return uri_dict


def upload_meta_front(attempt, cfg, uri_dict):
    """
    Send the extracted front to SciELO Manager

    :param attempt: Attempt object
    :param cfg: configuration file
    :param uri_dict: dict content the uri to the static file
    """

    client = scieloapi.Client(cfg.get('manager', 'api_username'),
                              cfg.get('manager', 'api_key'),
                              cfg.get('manager', 'api_url'), 'v1')


    ppl =  meta_extractor.get_meta_ppl()

    xml = PackageAnalyzer(attempt.filepath).xml

    data = {
        'front': next(ppl.run(xml, rewrap=True)),
        'xml_url': uri_dict['xml'],
        'pdf_url': uri_dict['pdf'],
    }

    client.articles.post(data)


def checkout_procedure(item):
    """
    This function performs some operations related to the checkout
        - Upload static files to the backend
        - Upload the front metadata to the Manager

    :param attempt: item (Attempt, config)
    """
    attempt, cfg = item

    attempt.checkout_started_at = datetime.now()

    uri_dict = upload_static_files(attempt, cfg)

    upload_meta_front(attempt, config, uri_dict)


if __name__ == '__main__':

    config = utils.Configuration.from_env()

    Session = models.Session
    Session.configure(bind=models.create_engine_from_config(config))
    session = Session()

    pool = ThreadPool(config.getint('checkout', 'thread_pool_size'))

    while True:

        attempts_checkout = session.query(models.Attempt).filter_by(
                                          proceed_to_checkout=True).all()

        #Process only if exists itens
        if attempts_checkout:

            checkout_lst = CheckoutList()

            try:
                for attempt in attempts_checkout:
                    if attempt.pending_checkout:
                        checkout_lst.append((attempt, config))

                #Execute the checkout procedure for each item
                pool.map(checkout_procedure, checkout_lst)

                transaction.commit()
            except:
                transaction.abort()
                raise

        time.sleep(config.getint('checkout', 'time') * 5)

    pool.close