all:
	python setup.py sdist

test:
	python setup.py test

pypi:
	twine upload --repository-url https://upload.pypi.org/legacy/ dist/*
