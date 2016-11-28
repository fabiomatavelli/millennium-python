#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Millennium Python SDK

This is an SDK to use with Millennium ERP.
"""

__author__ = "Fábio Matavelli <fabiomatavelli@gmail.com>"
__version__ = "1.0"

import requests
import datetime
import json
import re
from collections import namedtuple

api_host = None
api_protocol = None
api_url = None
api_timeout = None
wts_session = None

"""
Exceptions to handle errors
"""
class MillenniumException(Exception):
	pass

class NoConnection(MillenniumException):
	def __init__(self, host):
		super(NoConnection, self).__init__(
			"Sem acesso ao host {0}.".format(host))

class LoginFailed(MillenniumException):
	def __init__(self):
		super(LoginFailed, self).__init__(
			"Falha no login.")

class NotLoggedIn(MillenniumException):
	def __init__(self):
		super(NotLoggedIn, self).__init__(
			"Login nao efetuado.")

class MethodExecFailed(MillenniumException):
	def __init__(self, method, error=None):
		super(MethodExecFailed, self).__init__(
			"Erro ao executar o metodo '{0}': {1}".format(method, error or ""))

class BadParameter(MillenniumException):
	def __init__(self, method, error=None):
		super(BadParameter, self).__init__(
			"Um parametro no metodo '{0}' esta invalido. {1}".format(method, error or ""))

class MethodNotFound(MillenniumException):
	def __init__(self, method):
		super(MethodNotFound, self).__init__(
			"Metodo '{0}' inexistente.".format(method))

class MethodTimeout(MillenniumException):
	def __init__(self, method):
		super(MethodTimeout, self).__init__(
			"Timeout ao tentar executar o metodo '{0}'.".format(method))

def JSON2Datetime(obj):
	"""
	Method to convert JSON date to datetime object
	"""
	if isinstance(obj, dict):
		for k,v in obj.items():
			if isinstance(v, list):
				obj[k] = [JSON2Datetime(_obj) for _obj in v]
			elif isinstance(v, (unicode, str)):
				if re.match(r"^[0-9]{4}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}\:[0-9]{2}\:[0-9]{2}\.[0-9]{3}Z$", v) is not None:
					obj[k] = datetime.datetime.strptime(v, "%Y-%m-%dT%H:%M:%S.000Z")
	elif isinstance(obj, (str, unicode)):
		if re.match(r"^[0-9]{4}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}\:[0-9]{2}\:[0-9]{2}\.[0-9]{3}Z$", obj) is not None:
			obj = datetime.datetime.strptime(obj, "%Y-%m-%dT%H:%M:%S.000Z")
	return obj

def Datetime2JSON(obj):
	"""
	Method to convert datetime object to ISO datetime format
	"""
	if isinstance(obj, datetime.datetime):
		return obj.strftime("%Y-%m-%d %H:%M:%S")

def Dict2NamedTuple(obj, record=None):
	"""
	Convert dictionaries to named tuples
	"""
	if type(obj) == dict:
		record = "{0}Record".format(record.capitalize())
		exec("{0} = namedtuple(record, obj.keys())".format(record))
		for k, v in obj.iteritems():
			if type(v) in (unicode, str):
				exec("{0}.{1} = '{2}'".format(record,k,v.encode('utf-8')))
			else:
				exec("{0}.{1} = {2}".format(record,k,v))

		return eval(record)
	else:
		return obj

def Login(hostname,username,password,ssl=False,timeout=30):
	"""
	Do the login on the API and get the session token
	"""
	global wts_session, api_host, api_protocol, api_url, api_timeout

	api_host = hostname
	api_protocol = "https" if ssl else "http"
	api_url = "{0}://{1}/api".format(api_protocol, api_host)
	api_timeout = timeout or 30

	try:
		req = requests.get("{0}/login".format(api_url),
			params={"$format": "json"},timeout=api_timeout,
			headers={"WTS-Authorization": "{0}/{1}".format(username.upper(), password.upper())})
	except requests.exceptions.ConnectionError:
		raise NoConnection(api_host)
	else:
		if req.status_code == 401:
			raise LoginFailed()
		elif req.status_code == 500:
			raise MethodExecFailed("login",res.get("message"))
		elif req.status_code == 200:
			res = req.json()
			wts_session = res.get("session")
			assert "Logado com a sessão {0}.".format(wts_session)

def Call(method,method_type="GET",**kwargs):
	"""
	Function that calls Millennium methods
	"""
	
	if wts_session is None:
		raise NotLoggedIn()
	else:

		params = {"$format":"json","$dateformat":"iso"}

		try:
			if method_type.upper() == "GET":
				params.update(kwargs)
				req = requests.get("{0}/{1}".format(api_url,method), 
					params=params, headers={"WTS-Session": wts_session}, 
					timeout=api_timeout)
			else:
				req = requests.post("{0}/{1}".format(api_url,method), 
					data=json.dumps(kwargs), params=params, 
					headers={"WTS-Session": wts_session}, timeout=api_timeout)

		except requests.exceptions.ReadTimeout:
			raise MethodTimeout(method)
		else:
			if req.status_code == 400:
				raise BadParameter(method)
			elif req.status_code == 401:
				raise LoginFailed()
			elif req.status_code == 404:
				raise MethodNotFound(method)
			elif req.status_code == 500:
				raise MethodExecFailed(method,req.json().get("error").get("message").get("value"))
			else:
				res = req.json(object_hook=JSON2Datetime)

				if method_type.upper() == "GET":
					MillenniumResult = namedtuple("MillenniumResult", ["count", "result"])
					MillenniumResult.count = res.get("odata.count")
					MillenniumResult.result = (Dict2NamedTuple(result, "result") for result in res.get("value"))
				else:
					MillenniumResult = namedtuple("MillenniumResult", res.keys())
					MillenniumResult._make(res.values())
				
				return MillenniumResult

def Get(method, **kwargs):
	"""
	Function that get Millennium data
	"""
	return Call(method, method_type="GET", **kwargs)

def Post(method, **kwargs):
	"""
	Function that send data to Millennium
	"""
	return Call(method, method_type="POST", **kwargs)

if __name__ == "__main__":
	pass