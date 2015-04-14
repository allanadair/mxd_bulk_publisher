"""
mxd_bulk_publisher.pyt

This is a toolbox used for programatically publishing ArcMap documents as
services on ArcGIS server when migrating services from one machine to another.

"""

__version__ = '1.0'

from arcpy import AddMessage, env, Parameter
from arcpy.mapping import AnalyzeForSD, CreateGISServerConnectionFile, \
    CreateMapSDDraft, MapDocument
from arcpy.server import StageService, UploadServiceDefinition
import cPickle
from glob import iglob
import logging
from logging import Handler
import json
from os import sep
from os.path import basename, dirname, join
from urllib import urlencode
from urllib2 import Request
from urllib2 import urlopen
from uuid import uuid4
from xml.etree import ElementTree
from zipfile import ZipFile


class ToolboxLogHandler(Handler):
    """
    Custom logging handler for passing along messages.

    """
    def __init__(self):
        Handler.__init__(self)

    def emit(self, record):
        AddMessage(self.format(record))


def logger_init(instance, debug=False):
    """
    Shared function for initializing a custom logger.

    """
    if debug:
        # Set logging level to DEBUG
        instance.logger.setLevel(logging.DEBUG)

        # Console log handler
        logger = logging.StreamHandler()

    else:
        # Set logging level to INFO
        instance.logger.setLevel(logging.INFO)

        # Toolbox log handler
        logger = ToolboxLogHandler()

    # Create a universal logging formatter
    fmtstr = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmtstr)

    # Add formatter to console handler
    logger.setFormatter(formatter)

    # add handler to logger
    instance.logger.handlers = []  # Clear handlers. (ArcMap bug?)
    instance.logger.addHandler(logger)


class Configure(object):
    """
    Tool for creating custom configuration options for the bulk publisher tool.

    """
    def __init__(self, debug=False):
        """
        Initialization.

        """
        self.label = 'Configure'
        self.description = 'Tool for creating custom publishing configurations'
        self.canRunInBackground = True
        self.logger = logging.getLogger(self.label)
        logger_init(instance=self, debug=debug)

    def getParameterInfo(self):
        """
        Parameter definitions.

        """
        p0 = Parameter(displayName=u'ArcGIS server URL',
                       name='ags_url',
                       datatype='GPString',
                       parameterType='Required',
                       direction='Input',
                       category='ArcGIS server administration')
        p0.value = 'http://MyServer:6080/arcgis/admin'

        p1 = Parameter(displayName=u'Username',
                       name='username',
                       datatype='GPString',
                       parameterType='Required',
                       direction='Input',
                       category='ArcGIS server administration')
        p1.parameterDependencies = [p0.name]

        p2 = Parameter(displayName=u'Password',
                       name='password',
                       datatype='GPString',
                       parameterType='Required',
                       direction='Input',
                       category='ArcGIS server administration')
        p2.parameterDependencies = [p0.name]

        p3 = Parameter(displayName=u'Pooling minimum instances',
                       name='pool_min',
                       datatype='GPLong',
                       parameterType='Required',
                       direction='Input',
                       category='Service configuration options')
        p3.filter.type = 'ValueList'
        p3.filter.list = [0, 1, 2, 3, 4]
        p3.value = 1

        p4 = Parameter(displayName=u'Pooling maximum instances',
                       name='pool_max',
                       datatype='GPLong',
                       parameterType='Required',
                       direction='Input',
                       category='Service configuration options')
        p4.filter.type = 'ValueList'
        p4.filter.list = [0, 1, 2, 3, 4]
        p4.value = 4

        p5 = Parameter(displayName=u'Instances per container',
                       name='instances_container',
                       datatype='GPLong',
                       parameterType='Required',
                       direction='Input',
                       category='Service configuration options')
        p5.filter.type = 'ValueList'
        p5.filter.list = [1, 2, 3, 4, 5, 6, 7, 8]
        p5.value = 8

        p6 = Parameter(displayName=u'Process isolation',
                       name='process_isolation',
                       datatype='GPString',
                       parameterType='Required',
                       direction='Input',
                       category='Service configuration options')
        p6.filter.type = 'ValueList'
        p6.filter.list = ['high', 'low']
        p6.value = 'low'

        p7 = Parameter(displayName=u'Feature access enabled',
                       name='feature_access',
                       datatype='GPBoolean',
                       parameterType='Required',
                       direction='Input',
                       category='Service configuration options')
        p7.value = True

        p8 = Parameter(displayName=u'Maximum features returned',
                       name='max_features',
                       datatype='GPLong',
                       parameterType='Required',
                       direction='Input',
                       category='Service configuration options')
        p8.value = 1000

        p9 = Parameter(displayName=u'WFS enabled',
                       name='wfs_enabled',
                       datatype='GPBoolean',
                       parameterType='Required',
                       direction='Input',
                       category='Service configuration options')
        p9.value = True

        p10 = Parameter(displayName=u'Output deployment configuration file',
                        name='cfg_file',
                        datatype='DEFile',
                        parameterType='Required',
                        direction='Output',
                        category='Service configuration options')
        p10.filter.list = ['config']

        p11 = Parameter(displayName=u'Output ArcGIS connection file',
                        name='ags_file',
                        datatype='DEFile',
                        parameterType='Required',
                        direction='Output',
                        category='ArcGIS server administration')
        p11.filter.list = ['ags']
        p11.parameterDependencies = [p0.name, p1.name, p2.name]

        return [p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11]

    def execute(self, parameters, messages):
        """
        This is where the tool executes.

        """
        self.logger.info('Creating ArcGIS connection file...')
        try:
            CreateGISServerConnectionFile(connection_type='ADMINISTER_GIS_SERVICES',
                                          out_folder_path=dirname(unicode(parameters[11].value)),
                                          out_name=basename(unicode(parameters[11].value)),
                                          server_url=parameters[0].value,
                                          server_type='ARCGIS_SERVER',
                                          use_arcgis_desktop_staging_folder=True,
                                          staging_folder_path=None,
                                          username=parameters[1].value,
                                          password=parameters[2].value,
                                          save_username_password='SAVE_USERNAME')
        except Exception as e:
            self.logger.critical(unicode(e).replace('\n', ' '))

        try:
            self.logger.info('Determining cluster options...')

            # Generate Token
            request = 'generateToken'
            endpt = '/'.join((parameters[0].value, request))
            params = {'f': 'json',
                      'username': parameters[1].value,
                      'password': parameters[2].value,
                      'client': 'requestip',
                      'expiration': 1}
            data = urlopen(Request(url=endpt, data=urlencode(params)))
            response = json.loads(data.read())
            token = response.get('token')
            if not token:
                raise Exception('Unable to retrieve token from server.')

            # ArcGIS REST API query request to return available clusters
            request = 'clusters?{0}'
            params = {'f': 'json',
                      'token': token}
            request = request.format('&'.join(['{0}={1}'.format(k, v)
                                     for k, v in params.iteritems()]))
            endpt = '/'.join((parameters[0].value, request))
            data = urlopen(Request(url=endpt))
            response = json.loads(data.read())
            clusters = [cl['clusterName'] for cl in response['clusters']]

            self.logger.info('Saving configuration file...')

            cfg = {'clusters': clusters,
                   'pool_min': parameters[3].value,
                   'pool_max': parameters[4].value,
                   'instances_container': parameters[5].value,
                   'process_isolation': parameters[6].value,
                   'feature_access': parameters[7].value,
                   'max_features': parameters[8].value,
                   'wfs_enabled': parameters[9].value,

                   # For now, we are not giving the user the options below.
                   'ownership_based_access_control': False,
                   'allow_others_to_query': True,
                   'allow_others_to_update': False,
                   'allow_others_to_delete': False,
                   'allow_geometry_updates': False,
                   'kml_enabled': False}

            with open(unicode(parameters[10].value), 'wb') as destination:
                cPickle.dump(cfg, destination)

        except Exception as e:
            self.logger.critical(unicode(e).replace('\n', ' '))


class Publish(object):
    """
    Tool for publishing services.

    """
    def __init__(self, debug=False):
        """
        Initialization.

        """
        self.label = 'Publish'
        self.description = 'Tool for publishing services'
        self.canRunInBackground = True
        self.logger = logging.getLogger(self.label)
        logger_init(instance=self, debug=debug)

    def getParameterInfo(self):
        """
        Parameter definitions.

        """
        p0 = Parameter(displayName=u'ZIP file',
                       name='zip_file',
                       datatype='DEFile',
                       parameterType='Required',
                       direction='Input')
        p0.filter.list = ['zip']

        p1 = Parameter(displayName=u'SDE connection file',
                       name='sde_file',
                       datatype='DEFile',
                       parameterType='Required',
                       direction='Input')
        p1.filter.list = ['sde']

        p2 = Parameter(displayName=u'ArcGIS connection file',
                       name='ags_file',
                       datatype='DEFile',
                       parameterType='Required',
                       direction='Input')
        p2.filter.list = ['ags']

        p3 = Parameter(displayName=u'Deployment configuration file',
                       name='cfg_file',
                       datatype='DEFile',
                       parameterType='Required',
                       direction='Input')
        p3.filter.list = ['config']

        p4 = Parameter(displayName=u'Target cluster',
                       name='cluster',
                       datatype='GPString',
                       enabled=False,
                       parameterType='Required',
                       direction='Input')
        p4.parameterDependencies = [p3.name]
        p4.filter.type = 'ValueList'
        p4.filter.list = []

        return [p0, p1, p2, p3, p4]

    def updateParameters(self, parameters):
        if parameters[3].value and parameters[3].altered:
            cfg = cPickle.load(file(unicode(parameters[3].value)))
            parameters[4].filter.list = cfg['clusters']
            parameters[4].enabled = True

    def execute(self, parameters, messages):
        """
        This is where the tool executes.

        """
        env.overwriteOutput = True

        zip_file = parameters[0].value
        new_path = parameters[1].value
        ags_file = unicode(parameters[2].value)
        cfg = cPickle.load(file(unicode(parameters[3].value)))
        cluster = parameters[4].value
        scratch = join(env.scratchFolder, unicode(uuid4()))

        self.logger.debug('Path to zip file: {0}'.format(zip_file))
        self.logger.debug('Path to sde file: {0}'.format(new_path))
        self.logger.debug('Path to ags file: {0}'.format(ags_file))
        self.logger.debug('Path to config file: {0}'.format(parameters[3].value))
        self.logger.debug('Scratch folder: {0}'.format(scratch))

        with ZipFile(file=zip_file.value, mode='r') as zipf:
            zipf.extractall(scratch)

        for mxd_file in iglob('{0}{1}*{1}*.mxd'.format(scratch, sep)):

            # Point layers in mxd to the new database
            self.logger.debug('Path to mxd: {0}'.format(mxd_file))
            mxd = MapDocument(mxd_file)
            mxd.findAndReplaceWorkspacePaths(find_workspace_path='',
                                             replace_workspace_path=new_path,
                                             validate=True)
            self.logger.info('Updated paths in {0}'.format(basename(mxd_file)))
            mxd.save()
            self.logger.info('Saved changes to {0}'.format(basename(mxd_file)))

            # Create service definition draft
            name = basename(mxd_file).split('.mxd')[0]
            sddraft = '{0}.sddraft'.format(mxd_file)
            sd = '{0}.sd'.format(mxd_file)

            folder = basename(dirname(mxd_file))
            analysis = CreateMapSDDraft(map_document=mxd,
                                        out_sddraft=sddraft,
                                        service_name=name,
                                        server_type='FROM_CONNECTION_FILE',
                                        connection_file_path=ags_file,
                                        copy_data_to_server=False,
                                        folder_name=folder,
                                        summary=None,
                                        tags=None)
            self.logger.info('Analyzed {0}'.format(basename(mxd_file)))

            # Log messages, if any
            for k, v in analysis['messages'].items():
                for item in v:
                    self.logger.info('{0} - {1}'.format(item, k[0]))

            # Log warnings, if any
            for k, v in analysis['warnings'].items():
                for item in v:
                    self.logger.warn('{0} - {1}'.format(item, k[0]))

            # Log errors, if any
            for k, v in analysis['errors'].items():
                for item in v:
                    self.logger.error('{0} - {1}'.format(item, k[0]))

            # If there are errors, skip to the next mxd
            if analysis['errors']:
                self.logger.error('{0} will not be published'.format(name))
                continue

            # Alter sddraft with our deployment configuration settings
            tree = ElementTree.parse(sddraft)
            root = tree.getroot()

            # For some reason ElementTree drops these attributes, so here
            # we add them back in
            root.attrib['xmlns:xs'] = 'http://www.w3.org/2001/XMLSchema'
            root.attrib['xmlns:typens'] = 'http://www.esri.com/schemas/ArcGIS/10.1'

            # Most of our work will be under the "Definition"
            defn = root.find('Configurations').find('SVCConfiguration').find('Definition')

            # Service properties
            props = defn.find('Props').find('PropertyArray')
            for p in props:

                if p.find('Key').text == 'MinInstances':
                    p.find('Value').text = unicode(cfg['pool_min'])

                if p.find('Key').text == 'MaxInstances':
                    p.find('Value').text = unicode(cfg['pool_max'])

                if p.find('Key').text == 'InstancesPerContainer':
                    p.find('Value').text = unicode(cfg['instances_container'])

                if p.find('Key').text == 'Isolation':
                    p.find('Value').text = unicode(cfg['process_isolation'])

            # Extensions
            exts = defn.find('Extensions')
            for ex in exts:
                props = ex.find('Props').find('PropertyArray')

                # Feature server
                if ex.find('TypeName').text == 'FeatureServer':

                    # Set enabled to equal value in config
                    ex.find('Enabled').text = unicode(cfg['feature_access']).lower()

                    # Info requires a string of supported WebCapabilities
                    mapping = {'Query': cfg['allow_others_to_query'],
                               'Update': cfg['allow_others_to_update'],
                               'Delete': cfg['allow_others_to_delete']}

                    info_props = ex.find('Info').find('PropertyArray')

                    for p in info_props:
                        if p.find('Key').text == 'WebCapabilities':
                            p.find('Value').text = ','.join([k for k in mapping.keys() if mapping[k]])

                    # Modify service properties
                    for p in props:

                        if p.find('Key').text == 'maxRecordCount':
                            p.find('Value').text = unicode(cfg['max_features'])

                        if p.find('Key').text == 'enableOwnershipBasedAccessControl':
                            p.find('Value').text = unicode(cfg['ownership_based_access_control']).lower()

                        if p.find('Key').text == 'allowOthersToQuery':
                            p.find('Value').text = unicode(cfg['allow_others_to_query']).lower()

                        if p.find('Key').text == 'allowOthersToUpdate':
                            p.find('Value').text = unicode(cfg['allow_others_to_update']).lower()

                        if p.find('Key').text == 'allowOthersToDelete':
                            p.find('Value').text = unicode(cfg['allow_others_to_delete']).lower()

                        if p.find('Key').text == 'allowGeometryUpdates':
                            p.find('Value').text = unicode(cfg['allow_geometry_updates']).lower()

                # KML server
                if ex.find('TypeName').text == 'KmlServer':

                    # Set enabled to equal value in config
                    ex.find('Enabled').text = unicode(cfg['kml_enabled']).lower()

                # WFS server
                if ex.find('TypeName').text == 'WFSServer':

                    # Set enabled to equal value in config
                    ex.find('Enabled').text = unicode(cfg['wfs_enabled']).lower()

            # Set the cluster
            defn.find('Cluster').text = cluster

            # Save the sddraft
            with open(sddraft, 'w') as sddraft_file:
                tree.write(sddraft_file)

            # Analyze the new sddraft
            analysis = AnalyzeForSD(sddraft)

            # Log errors, if any
            for k, v in analysis['errors'].items():
                for item in v:
                    self.logger.error('{0} - {1}'.format(item, k[0]))

            # If there are errors, skip to the next mxd
            if analysis['errors']:
                self.logger.error('{0} will not be published'.format(name))
                continue

            # No analysis errors, so we can try to publish the service
            try:
                StageService(sddraft, sd)
                self.logger.info('Staged {0}'.format(basename(sddraft)))
                UploadServiceDefinition(sd, ags_file)
                self.logger.info('Uploaded {0}'.format(basename(sd)))
            except Exception as e:
                self.logger.critical(unicode(e).replace('\n', ' '))
                self.logger.critical('Unable to publish {0}'.format(name))


class Toolbox(object):
    """
    Mxd Bulk Publisher toolbox.

    """
    def __init__(self):
        self.label = 'Mxd Bulk Publisher {0}'.format(__version__)
        self.alias = 'mxd_bulk_publisher'
        self.description = 'Toolbox for bulk publishing services'
        self.tools = [Configure, Publish]
