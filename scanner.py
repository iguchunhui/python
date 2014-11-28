#! /usr/bin/python

from sys import argv
from os  import system
import os
import tarfile
import zipfile

#ipa nib file list
niblist= []
#project xib files list
xiblist= []


def addNibPath(nibpath):
    if len(nibpath) == 0:
        return
        
    for item in niblist:
        if cmp(item , nibpath) == 0:
            return
            
    niblist.append(nibpath)
    
    
def findIpaNib(ipaPath):
    zfile = zipfile.ZipFile(ipaPath,'r')
    if not zfile:
        print "%s is not ipa path" % ipaPath
        return
        
    namelist = zfile.namelist()
    for nl in namelist:
        pathComs = nl.split("/")
        for pathItem  in pathComs:
            if pathItem.find(".nib") > 0:
                addNibPath(pathItem)
                break
                
    zfile.close()

def displayNibs():
    print "----------nib files----------\n"
    for nib in niblist:
        print nib
    print "\n-----------------------------\n"

def findPojNib(path):

    path = os.path.normpath(path)
    
    baseframepath = "baseframe"
    if not path.endswith(baseframepath):
        subfiles = os.listdir(path)
        find = False
        for sf in subfiles:
            if cmp(sf,baseframepath) == 0 :
                find = True
                break
        if find :
            path = os.path.join(path,baseframepath)
        else :
            print "not find " , baseframepath , " input the right path"
            return            

    xiblistpath = '/tmp/111xib.txt'
    cmd = 'find  '+path + ' -name *.xib > ' + xiblistpath
    system(cmd)

    
    f = file(xiblistpath)
    if not f:
        print "open xib list file failed"
        return
    for line in f:
        xibname = os.path.split(line)[-1]
        if xibname.endswith('\n'):
            xibname = xibname[0:-1]
        if len(xibname) > 0:
            xiblist.append(xibname)

    f.close()
    os.remove(xiblistpath)
        

def showdiff():

    notinxiblist = []
    for xib in xiblist:
        name = xib[:-4]
        find = False
        for nib in niblist:
            nname = xib[:-4]
            if cmp(name , nname) == 0 :
                #find break
                find = True
                break
        if not find:
            notinxiblist.append(name)

    if len(notinxiblist) > 0 :
        print "******************************\n"
        print "Too Sad !!! \nNot in list: ",notinxiblist
        print "******************************\n"        
    else:
        print "******************************\n"        
        print "Congratulations!!!!"
        print "******************************\n"
    
def main():
    if len(argv) < 3:
        print "useage: scanner ipa_path proj_path"
        return
        
    path = argv[1]
    projpath = argv[2]

    
    findPojNib(projpath)
    findIpaNib(path)
    #displayNibs()
    showdiff()

if __name__ == '__main__':
    main()