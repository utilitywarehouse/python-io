import argparse
import importlib
import inspect
import os
import sys


def extract_module_name(path):
    relpath = os.path.relpath(path, root)
    return relpath.replace('.py', '').replace('/', '.')


def generate_item_doc(executable, class_name=None):
    content = ''
    signature = inspect.signature(executable)
    arg_list = []
    for key, value in signature.parameters.items():
        if value.kind == value.VAR_POSITIONAL:
            arg_list.append(f'*{key}')
        elif value.kind == value.VAR_KEYWORD:
            arg_list.append(f'**{key}')
        elif value.default == value.empty:
            arg_list.append(key)
        else:
            arg_list.append(f'{key}={value.default}')
    arg_str = ', '.join(arg_list)
    content += '```python\n'
    if class_name:
        path = f'{executable.__module__}.{class_name}.{executable.__name__}'
    else:
        path = f'{executable.__module__}.{executable.__name__}'
    content += f'# {path}\n'
    content += f'{executable.__name__}({arg_str})'
    content += '\n```\n'
    if (docstring := inspect.getdoc(executable)):
        content += '\n```\n'
        content += inspect.getdoc(docstring)
        content += '\n```\n\n'
    return content


def save_file(name, content):
    with open(os.path.join(docs_root, f'{name}.md'), 'w') as infile:
        infile.write(content)


parser = argparse.ArgumentParser()
parser.add_argument('output_dir')
args = parser.parse_args()

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root not in sys.path:
    sys.path.append(root)
iolib_root = os.path.join(root, 'iolib')
docs_root = os.path.join(root, args.output_dir)
if not os.path.exists(docs_root):
    os.makedirs(docs_root)
else:
    for filename in os.listdir(docs_root):
        os.remove(os.path.join(docs_root, filename))

for dirpath, _, filenames in os.walk(iolib_root):
    for filename in filenames:
        if filename.startswith('_') or not filename.endswith('.py'):
            continue
        path = os.path.join(dirpath, filename)
        module_name = extract_module_name(path)
        module = importlib.import_module(module_name)
        content = ''

        # Render classes documentation.
        for class_name, cls in inspect.getmembers(module, inspect.isclass):
            # Exclude imported classes.
            if cls.__module__ != module_name:
                continue
            content = ''
            content += f'## {class_name}\n\n'
            if (docstring := inspect.getdoc(cls)):
                content += f'```\n{docstring}\n```\n\n'
            for obj_name, obj in inspect.getmembers(cls):
                if obj_name.startswith('_') or \
                        not (inspect.ismethod(obj) or inspect.isfunction(obj)):
                    continue
                content += f'### {obj_name}\n\n'
                content += generate_item_doc(obj, class_name=class_name)

        # Render functions documentation.
        for obj_name, obj in inspect.getmembers(module, inspect.isfunction):
            content += f'## {obj_name}\n\n'
            content += generate_item_doc(obj)

        save_file(module_name, content)
