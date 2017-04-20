#coding=utf-8
import os
from fabric.api import local, cd, lcd, run, put, env

env.hosts=['liliang@192.168.8.30:22',] #ssh要用到的参数
env.password = 'good#4213*'

def hello():
    print("hello world")

def deploy():
    local_app_dir = "d:/work/test/ccpush/mr2p/"
    remote_app_dir = "/opt/tools"

    # lcd 是 「local cd」，cd 是在远程服务器执行 cd
    with lcd(local_app_dir), cd(remote_app_dir):
        path = os.path.join(remote_app_dir, "mr2p")
        run("mkdir -p %s" % path)
        # put 把本地文件传输到远程（ SFTP (SSH File Transfer Protocol) 协议）
        put("d:/work/test/ccpush/mr2p/*", path)
