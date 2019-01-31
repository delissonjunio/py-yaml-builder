from io import IOBase
import os
import yaml

AWS_CLOUDFORMATION_BUILTINS = [
    'Ref', 'Base64', 'Cidr', 'FindInMap', 'GetAtt', 'GetAZs', 'ImportValue', 'Join',
    'Select', 'Split', 'Sub', 'Transform', 'And', 'Equals', 'If', 'Not', 'Or'
]

class ComposeLoader(yaml.Loader):
    def __init__(self, *args, **kwargs):
        super(ComposeLoader, self).__init__(*args, **kwargs)

        self.add_constructor('!include', self._include)
        self.add_constructor('!import', self._include)
        self.add_constructor('!env', self._env_var)

        for cloudformation_builtin in AWS_CLOUDFORMATION_BUILTINS:
            self.add_constructor('!' + cloudformation_builtin, self._cloudformation_builtin)

        if 'root' in kwargs:
            self._root = kwargs['root']
        elif isinstance(self.stream, IOBase):
            self._root = os.path.dirname(self.stream.name)
        else:
            self._root = os.path.curdir

    def _get_file(self, file_name):
        file_path = os.path.join(self._root, file_name)
        with open(file_path, 'r') as yaml_file:
            return yaml.load(yaml_file, Loader=ComposeLoader)

    def _env_var(self, loader, node):
        env_var = loader.construct_scalar(node)
        return os.getenv(env_var)

    def _cloudformation_builtin(self, loader, node):
        if isinstance(node, yaml.ScalarNode):
            value = loader.construct_scalar(node).encode('utf-8')
        elif isinstance(node, yaml.SequenceNode):
            value = loader.construct_sequence(node)
        elif isinstance(node, yaml.MappingNode):
            value = loader.construct_mapping(node)
        else:
            raise yaml.YAMLError('Unknown node type for {}'.format(node.tag))

        full_function_name = str(node.tag.replace('!', 'Fn::'))
        return {full_function_name: value}

    def _include(self, loader, node):

        if isinstance(node, yaml.SequenceNode):
            include_data = [
                self._get_file(loader.construct_scalar(seq_node))
                for seq_node in node.value
            ]
        else:
            include_data = [self._get_file(loader.construct_scalar(node))]

        data_types = set([type(data) for data in include_data])

        if len(data_types) > 1:
            raise ValueError('Found mixed data types in list include')

        return_data_type = data_types.pop()
        return_data = return_data_type()

        for data_to_include in include_data:
            if isinstance(return_data, list):
                return_data.append(data_to_include)
            else:
                return_data.update(data_to_include)

        return return_data
