from distutils.core import setup

setup(
    name='bulk-miner',
    version='1.0',
    packages=['bulk'],
    url='https://github.com/DANS-repo/bulk-miner',
    license='Apache 2.0',
    author='hvdb',
    author_email='',
    description='ad hoc script to extract certain file ids',
    install_requires=['fedora-api']
)