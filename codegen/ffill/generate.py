import os

import jinja2

from cloud_validol.lib import datasets


CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


def main():
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(os.path.join(CURRENT_DIR, 'templates')),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = env.get_template('views.jinja')

    return template.render(views=datasets.SCHEMA)


if __name__ == '__main__':
    print(main())
