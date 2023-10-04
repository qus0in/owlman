from setuptools import setup, find_packages
import owlman

setup(
    name='owlman',
    version=owlman.__version__,
    description='금융 분석을 위한 툴 모음',
    author='qus0in',
    author_email='qus0in@gmail.com',
    url='https://github.com/qus0in/owlman',
    install_requires=['requests', 'pandas', 'plotly', 'scikit-learn'],
    packages=find_packages(exclude=[]),
    keywords=['owlman'],
    python_requires='>=3.6',
    package_data={},
    zip_safe=False,
)