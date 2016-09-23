from setuptools import setup
import sys

long_description = None
if 'upload' in sys.argv or 'register' in sys.argv:
    from pypandoc import convert
    long_description = convert('README.md', 'rst')

setup(
    name='gevent-pipeline',
    packages=['gevent_pipeline'],

    use_scm_version=True,
    setup_requires=['setuptools_scm', 'pytest-runner'],

    install_requires=['gevent'],
    tests_require=['pytest'],

    description='Multi-worker pipeline and closable queue',
    long_description=long_description,

    url='https://github.com/simonschmidt/gevent-pipeline',
    author='Simon Schmidt',
    author_email='schmidt.simon@gmail.com',

    # Choose your license
    license='ISC',

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: ISC License (ISCL)',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],

    keywords='gevent pipelines chaining closablequeue',
)
