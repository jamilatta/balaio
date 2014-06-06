import unittest

import mocker
from sqlalchemy.exc import OperationalError

from balaio.lib.notifier import Notifier
from balaio.lib import models
from . import doubles, modelfactories
from .utils import db_bootstrap, DB_READY


global_engine = None


def setUpModule():
    """
    Initialize the database.
    """
    global global_engine
    try:
        global_engine = db_bootstrap()
    except OperationalError:
        # global_engine remains None, all db-bound testcases
        # need to test for DB_READY before run.
        pass


class NotifierTests(mocker.MockerTestCase):

    def _makeOne(self, **kwargs):
        checkpoint = kwargs.get('checkpoint', modelfactories.CheckpointFactory())
        scieloapi = kwargs.get('scieloapi', doubles.ScieloAPIClientStub())
        db_session = kwargs.get('db_session', doubles.SessionStub())

        return Notifier(checkpoint, scieloapi, db_session)

    @unittest.skipUnless(DB_READY, u'DB must be set. Make sure `app_balaio_tests` is properly configured.')
    def test_start_sends_notification_on_checkin_points(self):
        checkpoint = modelfactories.CheckpointFactory(point=models.Point.checkin)
        notifier = self._makeOne(checkpoint=checkpoint)

        mock_notifier = self.mocker.patch(notifier)
        mock_notifier._send_checkin_notification()
        self.mocker.result(None)

        mock_notifier._send_notice_notification('', models.Status.SERV_BEGIN)
        self.mocker.result(None)

        self.mocker.replay()

        notifier.start()

    @unittest.skipUnless(DB_READY, u'DB must be set. Make sure `app_balaio_tests` is properly configured.')
    def test_start_doesnt_send_notification_otherwise(self):
        checkpoint = modelfactories.CheckpointFactory(point=models.Point.validation)
        notifier = self._makeOne(checkpoint=checkpoint)

        mock_notifier = self.mocker.patch(notifier)
        mock_notifier._send_checkin_notification()
        self.mocker.result(None)
        self.mocker.count(0,0)  # means the method is not called

        mock_notifier._send_notice_notification('', models.Status.SERV_BEGIN)
        self.mocker.result(None)

        self.mocker.replay()

        notifier.start()

    @unittest.skipUnless(DB_READY, u'DB must be set. Make sure `app_balaio_tests` is properly configured.')
    def test_send_checkout_notification_payload(self):
        checkpoint = modelfactories.CheckpointFactory(point=models.Point.checkout)

        expected = {
            'checkin': None,
            'stage': 'checkout',
            'checkpoint': 'checkout',
            'message': 'checkout finished',
            'status': 'ok',
        }

        mock_scieloapi = self.mocker.mock()
        mock_scieloapi.notices.post(expected)
        self.mocker.result(None)
        self.mocker.replay()

        notifier = self._makeOne(checkpoint=checkpoint, scieloapi=mock_scieloapi)
        self.assertIsNone(notifier._send_checkout_notification())

    @unittest.skipUnless(DB_READY, u'DB must be set. Make sure `app_balaio_tests` is properly configured.')
    def test_send_checkin_notification_payload(self):
        attempt = modelfactories.AttemptFactory()
        checkpoint = modelfactories.CheckpointFactory(point=models.Point.checkin, attempt=attempt)

        expected1 = {
             'articlepkg_ref': str(checkpoint.attempt.articlepkg.id),
             'article_title': checkpoint.attempt.articlepkg.article_title,
             'journal_title': checkpoint.attempt.articlepkg.journal_title,
             'issue_label': checkpoint.attempt.articlepkg.issue_label,
             'pissn': checkpoint.attempt.articlepkg.journal_pissn,
             'eissn': checkpoint.attempt.articlepkg.journal_eissn,
        }

        expected2 = {
             'attempt_ref': str(checkpoint.attempt.id),
             'package_name': '0042-9686-bwho-91-08-545',
             'uploaded_at': str(checkpoint.attempt.started_at),
             'article': '/api/v1/checkins_articles/1/',
             'submitted_by': str(checkpoint.attempt.submitted_by),
        }

        mock_attempt = self.mocker.patch(attempt)
        mock_attempt.analyzer.get_ext('xml')
        self.mocker.result(['0042-9686-bwho-91-08-545/0042-9686-bwho-91-08-545.xml'])

        mock_scieloapi = self.mocker.mock()
        mock_scieloapi.checkins_articles.post(expected1)
        self.mocker.result(1)  # resource id
        mock_scieloapi.checkins.post(expected2)
        self.mocker.result(2)  # resource id
        self.mocker.replay()

        notifier = self._makeOne(checkpoint=checkpoint, scieloapi=mock_scieloapi)
        self.assertIsNone(notifier._send_checkin_notification())

    @unittest.skipUnless(DB_READY, u'DB must be set. Make sure `app_balaio_tests` is properly configured.')
    def test_send_checkin_notification_handles_scieloapi_exc(self):
        from scieloapi.exceptions import APIError

        checkpoint = modelfactories.CheckpointFactory(point=models.Point.checkin)

        mock_scieloapi = self.mocker.mock()
        mock_scieloapi.checkins_articles.post(mocker.ANY)
        self.mocker.throw(APIError)
        self.mocker.replay()

        notifier = self._makeOne(checkpoint=checkpoint, scieloapi=mock_scieloapi)
        self.assertIsNone(notifier._send_checkin_notification())

    @unittest.skipUnless(DB_READY, u'DB must be set. Make sure `app_balaio_tests` is properly configured.')
    def test_send_notice_notification_on_checkin_points(self):
        checkpoint = modelfactories.CheckpointFactory(point=models.Point.checkin)
        notifier = self._makeOne(checkpoint=checkpoint)

        mock_notifier = self.mocker.patch(notifier)

        mock_notifier._send_checkin_notification()
        self.mocker.result(None)

        mock_notifier._send_notice_notification('', models.Status.SERV_BEGIN)
        self.mocker.result(None)

        mock_notifier._send_notice_notification('foo', models.Status.ok, label='bar')
        self.mocker.result(None)

        self.mocker.replay()

        notifier.start()
        notifier.tell('foo', models.Status.ok, label='bar')

    @unittest.skipUnless(DB_READY, u'DB must be set. Make sure `app_balaio_tests` is properly configured.')
    def test_send_notice_notification_payload(self):
        checkpoint = modelfactories.CheckpointFactory(point=models.Point.validation)
        checkpoint.attempt.checkin_uri = '/api/v1/checkins/1/'

        expected = {
            'checkin': '/api/v1/checkins/1/',
            'stage': 'bar',
            'checkpoint': 'validation',
            'message': 'foo',
            'status': 'ok',
        }

        mock_scieloapi = self.mocker.mock()
        mock_scieloapi.notices.post(expected)
        self.mocker.result(None)
        self.mocker.replay()

        notifier = self._makeOne(checkpoint=checkpoint, scieloapi=mock_scieloapi)
        self.assertIsNone(notifier._send_notice_notification(
            'foo', models.Status.ok, label='bar'))

    @unittest.skipUnless(DB_READY, u'DB must be set. Make sure `app_balaio_tests` is properly configured.')
    def test_send_notice_notification_handles_scieloapi_exc(self):
        from scieloapi.exceptions import APIError

        checkpoint = modelfactories.CheckpointFactory(point=models.Point.validation)
        checkpoint.attempt.checkin_uri = '/api/v1/checkins/1/'

        mock_scieloapi = self.mocker.mock()
        mock_scieloapi.notices.post(mocker.ANY)
        self.mocker.throw(APIError)
        self.mocker.replay()

        notifier = self._makeOne(checkpoint=checkpoint, scieloapi=mock_scieloapi)
        self.assertIsNone(notifier._send_notice_notification(
            'foo', models.Status.ok, label='bar'))

