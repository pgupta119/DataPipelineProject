from setuptools import setup, find_packages


def read_me():
    with open('ETL_README.md') as f:
        README = f.read()
    return README


setup(name='pipelines',
      version='0.0.1',
      description='ETL Pipeline framework',
      long_description=read_me(),
      url='https://github.com/xyz',
      author='prakash gupta',
      author_email='p.guptaprakash94@gmail.com',
      classifiers=[
          'Programming Language :: Python :: 3.6'
          'Programming Language :: Python :: 3.8.2'],
      packages=['pipeline', 'pipeline.utils', 'pipeline.helper'],
      include_package_data=True,
      install_requires=['isodate', 'Pyspark', 'pandas'],
      zip_safe=False)
