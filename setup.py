from setuptools import setup

setup(
	name = "eaquery",
	packages = ["eaquery"],
	py_modules =["eaquery"],
	version = "0.1.0",
	install_requires=[
		"simple_salesforce",
		"pandas",
		"requests"
	]
)