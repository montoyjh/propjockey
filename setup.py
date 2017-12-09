from setuptools import setup

setup(
    name='propjockey',
    packages=['propjockey'],
    include_package_data=True,
    install_requires=[
        'flask',
        'pymongo',
        'requests',
        'toolz',
    ],
    setup_requires=[
        'pytest-runner',
    ],
    tests_require=[
        'six',
        'pytest',
    ],
)
