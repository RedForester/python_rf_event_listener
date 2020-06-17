from setuptools import setup, find_packages

with open('README.md') as f:
    long_description = f.read()

setup(
    name='rf_api_client',
    version='0.0.1',
    description='RedForester Event Listener',
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
    ],
    url='https://github.com/RedForester/rf_event_listener',
    author='Red Forester',
    author_email='tech@redforester.com',
    license='MIT',
    packages=find_packages(),
    python_requires='>=3.6',
    install_requires=[
        'pydantic ~= 1.5',
        'rf-api-client >= 0.0'
    ],
    include_package_data=True,
    zip_safe=False
)
