import os

import jinja2


CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
VIEWS = [
    {
        'name': 'fredgraph',
        'dimension_columns': [],
        'measure_columns': [
            'mbase',
            'tdebt',
        ],
    },
    {
        'name': 'investing_prices',
        'dimension_columns': ['currency_cross'],
        'measure_columns': [
            'open_price',
            'high_price',
            'low_price',
            'close_price',
        ],
    },
    {
        'name': 'moex_derivatives',
        'dimension_columns': ['derivative_name'],
        'measure_columns': ['fl', 'fs', 'ul', 'us', 'flq', 'fsq', 'ulq', 'usq'],
    },
    {
        'name': 'cot_futures_only',
        'dimension_columns': [
            'platform_source',
            'platform_code',
            'platform_name',
            'derivative_name',
            'report_type',
        ],
        'measure_columns': [
            'oi',
            'ncl',
            'ncs',
            'cl',
            'cs',
            'nrl',
            'nrs',
            'x_4l_percent',
            'x_4s_percent',
            'x_8l_percent',
            'x_8s_percent',
        ],
    },
    {
        'name': 'cot_disaggregated',
        'dimension_columns': [
            'platform_source',
            'platform_code',
            'platform_name',
            'derivative_name',
            'report_type',
        ],
        'measure_columns': [
            'oi',
            'nrl',
            'nrs',
            'pmpl',
            'pmps',
            'sdpl',
            'sdps',
            'mmpl',
            'mmps',
            'orpl',
            'orps',
            'x_4gl_percent',
            'x_4gs_percent',
            'x_8gl_percent',
            'x_8gs_percent',
            'x_4l_percent',
            'x_4s_percent',
            'x_8l_percent',
            'x_8s_percent',
            'sdp_spr',
            'mmp_spr',
            'orp_spr',
            'cl',
            'cs',
            'ncl',
            'ncs',
        ],
    },
    {
        'name': 'cot_financial_futures',
        'dimension_columns': [
            'platform_source',
            'platform_code',
            'platform_name',
            'derivative_name',
            'report_type',
        ],
        'measure_columns': [
            'oi',
            'dipl',
            'dips',
            'dip_spr',
            'ampl',
            'amps',
            'amp_spr',
            'lmpl',
            'lmps',
            'lmp_spr',
            'orpl',
            'orps',
            'orp_spr',
            'nrl',
            'nrs',
        ],
    },
]


def main():
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(os.path.join(CURRENT_DIR, 'templates')),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = env.get_template('views.jinja')

    return template.render(views=VIEWS)


if __name__ == '__main__':
    print(main())
