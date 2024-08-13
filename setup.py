#!/usr/bin/env python

import re, os, sys, time, datetime, platform, pkg_resources;
from setuptools import setup, find_packages;

verinfofilename = os.path.realpath("."+os.path.sep+os.path.sep+"parse_message_file.py");
verinfofile = open(verinfofilename, "r");
verinfodata = verinfofile.read();
verinfofile.close();
setuppy_verinfo_esc = re.escape("__version_info__ = (")+"(.*)"+re.escape(");");
setuppy_verinfo = re.findall(setuppy_verinfo_esc, verinfodata)[0];
setuppy_verinfo_exp = [vergetspt.strip().replace("\"", "") for vergetspt in setuppy_verinfo.split(',')];
setuppy_dateinfo_esc = re.escape("__version_date_info__ = (")+"(.*)"+re.escape(");");
setuppy_dateinfo = re.findall(setuppy_dateinfo_esc, verinfodata)[0];
setuppy_dateinfo_exp = [vergetspt.strip().replace("\"", "") for vergetspt in setuppy_dateinfo.split(',')];
pymodule = {};
pymodule['version'] = str(setuppy_verinfo_exp[0])+"."+str(setuppy_verinfo_exp[1])+"."+str(setuppy_verinfo_exp[2]);
pymodule['versionrc'] = int(setuppy_verinfo_exp[4]);
pymodule['versionlist'] = (int(setuppy_verinfo_exp[0]), int(setuppy_verinfo_exp[1]), int(setuppy_verinfo_exp[2]), str(setuppy_verinfo_exp[3]), int(setuppy_verinfo_exp[4]));
pymodule['verdate'] = str(setuppy_dateinfo_exp[0])+"."+str(setuppy_dateinfo_exp[1])+"."+str(setuppy_dateinfo_exp[2]);
pymodule['verdaterc'] = int(setuppy_dateinfo_exp[4]);
pymodule['verdatelist'] = (int(setuppy_dateinfo_exp[0]), int(setuppy_dateinfo_exp[1]), int(setuppy_dateinfo_exp[2]), str(setuppy_dateinfo_exp[3]), int(setuppy_dateinfo_exp[4]));
pymodule['name'] = 'LoveSoStrong';
pymodule['author'] = 'Yehoshua35';
pymodule['authoremail'] = 'yehoshua35@gmail.com';
pymodule['maintainer'] = 'Yehoshua35';
pymodule['maintaineremail'] = 'yehoshua35@gmail.com';
pymodule['description'] = 'Love so Strong it\'s Creepy 😳';
pymodule['license'] = 'Revised BSD License';
pymodule['keywords'] = 'catfile pycatfile python python-catfile';
pymodule['url'] = 'https://github.com/Yehoshua35/LoveSoStrong';
pymodule['downloadurl'] = 'https://github.com/Yehoshua35/LoveSoStrong/archive/master.tar.gz';
pymodule['packages'] = find_packages();
pymodule['packagedata'] = {'data': ['*.txt']};
pymodule['longdescription'] = 'love loveisokifnotextreme extremeloveisnotok lovesostrong lovesostrongitscreepy lovesostrongitiscreepy extreamelove excessivelove yanderelove unbendinglove loveyoucantbelievein whydidthishappentomelove creepylove loveinabundance morelovemoreextreme weheardyoulikelovesowegotyoulove iloveyoumorethenyouknow ifyoulovethemtheywilllovebackinextreme whenyoulovetheylovebackinextreme ifonlyineverlovedagain somuchloveyoucanthandleitanddie weloveonlyforlovetheyloveforextremelove iloveyoumorethenyouknowbutyouloveinextreme isextremeloverealyinhighdemand lovesostrongitscreepy lovesostrongitiscreepy extreamelove excessivelove yanderelove unbendinglove loveyoucantbelievein whydidthishappentomelove creepylove loveinabundance isloverealyinhighdemand morelovemoreextreme weheardyoulikelovesowegotyoulove iloveyoumorethenyouknow ifyoulovethemtheywilllovebackinextreme whenyoulovetheylovebackinextreme ifonlyineverlovedagain somuchloveyoucanthandleitanddie weloveonlyforlovetheyloveforextremelove iloveyoumorethenyouknowbutyouloveinextreme willidiefromallthisextremelove extremeloveyoulldiefor whydotheylovemesoextreme ionlyloveyoubutyoutookittoextremes somuchloveitsunhealthy unhealthylove whydidmylovemakethemloveinextremeamounts cantheylovemeanymoreifitsinextremeamounts willtheyeverstoplovingmeinextremeamounts extremelovestory';
pymodule['platforms'] = 'OS Independent';
pymodule['zipsafe'] = True;
pymodule['pymodules'] = ['parse_message_file'];
pymodule['scripts'] = ['nextest.py', 'parse_message_file.py'];
pymodule['classifiers'] = [
 'Development Status :: 5 - Production/Stable',
 'Intended Audience :: Developers',
 'Intended Audience :: Other Audience',
 'License :: OSI Approved',
 'License :: OSI Approved :: BSD License',
 'Natural Language :: English',
 'Operating System :: MacOS',
 'Operating System :: MacOS :: MacOS X',
 'Operating System :: Microsoft',
 'Operating System :: Microsoft :: Windows',
 'Operating System :: OS/2',
 'Operating System :: OS Independent',
 'Operating System :: POSIX',
 'Operating System :: Unix',
 'Programming Language :: Python',
 'Topic :: Utilities',
 'Topic :: Software Development',
 'Topic :: Software Development :: Libraries',
 'Topic :: Software Development :: Libraries :: Python Modules'
];

setup(
 name = pymodule['name'],
 version = pymodule['version'],
 author = pymodule['author'],
 author_email = pymodule['authoremail'],
 maintainer = pymodule['maintainer'],
 maintainer_email = pymodule['maintaineremail'],
 description = pymodule['description'],
 license = pymodule['license'],
 keywords = pymodule['keywords'],
 url = pymodule['url'],
 download_url = pymodule['downloadurl'],
 long_description = pymodule['longdescription'],
 platforms = pymodule['platforms'],
 zip_safe = pymodule['zipsafe'],
 py_modules = pymodule['pymodules'],
 scripts = pymodule['scripts'],
 classifiers = pymodule['classifiers']
)
