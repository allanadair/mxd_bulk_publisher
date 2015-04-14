from arcpy import ImportToolbox
import imp
from os import getcwd
from os.path import basename, join

# Toolbox path
tbx_path = 'mxd_bulk_publisher.pyt'

# Configuration structure used for debugging with default parameters
cfg = {'Configure': {'init': {'debug': True},
                     'parameters': [('ags_url', 'http://mygisserver.com:6080/arcgis/admin'),
                                    ('username', 'gisadminuser'),
                                    ('password', 'gisadminpassword'),
                                    ('pool_min', 1),
                                    ('pool_max', 4),
                                    ('instances_container', 8),
                                    ('process_isolation', 'low'),
                                    ('feature_access', True),
                                    ('max_features', 1000),
                                    ('wfs_enabled', True),
                                    ('cfg_file', join(getcwd(),
                                                      'sample',
                                                      'mygisserver.config')),
                                    ('ags_file', join(getcwd(),
                                                      'sample',
                                                      'mygisserver.ags'))]
                     },
       'Publish': {'init': {'debug': True},
                   'parameters': [('zip_file', join(getcwd(),
                                                    'sample',
                                                    'sample.zip')),
                                  ('sde_file', join(getcwd(),
                                                    'sample',
                                                    'mygisdb.sde')),
                                  ('ags_file', join(getcwd(),
                                                    'sample',
                                                    'mygisserver.ags')),
                                  ('cfg_file', join(getcwd(),
                                                    'sample',
                                                    'mygisserver.config')),
                                  ('cluster', 'default')]
                   }
       }

if __name__ == '__main__':
    tbx_info = ImportToolbox(tbx_path)
    tbx = imp.load_source('.'.join(basename(tbx_path).split('.')[:-1]),
                          tbx_path)
    for tool_name in tbx_info.__all__:
        tool_opts = cfg.get(tool_name)
        if tool_opts:
            tool_cls = getattr(tbx, tool_name)
            tool = tool_cls(**tool_opts['init'])

            debug_params = dict(tool_opts['parameters'])
            parameters = tool.getParameterInfo()

            for param in parameters:
                param.value = debug_params[param.name]

            tool.execute(parameters=parameters,
                         messages=None)
