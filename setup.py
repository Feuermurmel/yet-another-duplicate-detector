import setuptools


setuptools.setup(
    name='yet-another-duplicate-detector',
    version='1.0.0',
    packages=['yadd'],
    entry_points=dict(
        console_scripts=['yadd = yadd:entry_point']),
    install_requires=[])
