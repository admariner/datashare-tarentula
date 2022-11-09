from click.testing import CliRunner

from .test_abstract import TestAbstract
from tarentula.cli import cli
from tarentula import __version__

class TestVersion(TestAbstract):

    def test_version(self):
        runner = CliRunner()
        result = runner.invoke(cli, ['--version'])
        self.assertIn(f'v{__version__}', result.output)

    def test_version_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ['--help'])
        self.assertIn('Show the version and exit.', result.output)
