from setuptools import setup, find_packages

setup(
    name='data-pipeline-framework',
    version='0.1.0',
    description='Framework Python para pipelines de dados',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/seu_usuario/seu_repositorio',
    author='Seu Nome',
    author_email='seu_email@example.com',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    keywords='data pipeline framework python',
    packages=find_packages(),
    install_requires=[
        # Coloque as dependências necessárias aqui
    ],
    python_requires='>=3.6',
)
