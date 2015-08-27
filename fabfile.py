"""
    An example which makes use of the search-interface component. This launches a host
    which has the search interface and provides a test search interface.
"""

from fabric.api import task, local, run

import cliqz
import cliqz_tasks  # It is important to have the line (used by fabric)

app_name = 'query_embeddings'

'''
@task
def install_server_upstart():
    cliqz.cli.add_upstart_service(app_name, 'conf/upstart.tpl.conf')

'''
@task
def full_install(host_details=None):
    """Installs and launches stuff"""
    #pkg = cliqz.package.gen_definition()
    #local("tar cjf {} modules".format(pkg['local']))
    
    cliqz.cli.system_package('python-pip', 'gcc', 'make','build-essential','python-setuptools','libatlas-dev','libatlas3gf-base', 'python-dev','python-numpy', 'python-scipy', 'python-pandas', 'python-sympy', 'python-nose')
    cliqz.cli.python_package(
        'pyyaml',
        'gunicorn==18.0',
        'requests==1.2.3',
        'mrjob==0.4.5',
	'msgpack',
	'cython',
	'joblib',
	'scikit-learn'
	
    )
    
    #cliqz.package.install(pkg, '/opt/' + app_name)
    #install_server_upstart()  # First time should be launched via ssh
    #run('mkdir -p /var/lib/hackday-trending')
    #run('mkdir -p /var/lib/hackday-trending/source/')
    #run('mkdir -p /var/lib/hackday-trending/processed/')
    #run('service hackday-trending restart')
    pass

@task
def redeploy():
    """Installs and launches stuff"""
    pkg = cliqz.package.gen_definition()
    local("tar cjf {} modules".format(pkg['local']))
    cliqz.package.install(pkg, '/opt/' + app_name)
    run('service hackday-trending restart')


tcp = [('ip_protocol', 'tcp')]
http_ports = tcp + [('from_port', 80), ('to_port', 80)]

cliqz.setup(
    app_name=app_name,
    project_owners=['josep','ankit'],
    buckets=['josep-test'],
    policies=[],
    cluster={
        'primary_install': full_install,
        'vpc_id': 'vpc-f0733595',
        'name': app_name,
        'image': 'ubuntu-14.04-64bit',
        'instances': [
            {
                'price_zone': 'c1',
                'spot_price': 1.5,
                'ebs_size': 128,
                'ebs_type': 'gp2',
                'num_instances': 1,
                'instance_type': 'c3.8xlarge',
                'placement': 'us-east-1a',
            },
        ],
    },
    security_rules=[
        dict(http_ports + [('cidr_ip', '0.0.0.0/0')]),
    ],
)
