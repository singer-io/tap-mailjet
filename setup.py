

from setuptools import setup, find_packages


setup(name="tap-mailjet",
      version="0.0.1",
      description="Singer.io tap for extracting data from mailjet API",
      author="Stitch",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_mailjet"],
      install_requires=[
        "singer-python==6.1.1",
        "requests==2.32.4",
        "backoff==2.2.1",
        "parameterized"
      ],
      entry_points="""
          [console_scripts]
          tap-mailjet=tap_mailjet:main
      """,
      packages=find_packages(),
      package_data = {
          "tap_mailjet": ["schemas/*.json"],
      },
      include_package_data=True,
)
