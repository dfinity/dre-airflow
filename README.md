# StackStorm pack for the DRE team

The StackStorm pack with objects used by the Distributed Reliability Team.

This StackStorm pack is deployed to the DRE StackStorm instance.

General information on packs:

* https://docs.stackstorm.com/packs.html
* https://docs.stackstorm.com/reference/packs.html

## Local development environment

With `pip3` on your system, run:

```
pip3 install --user git+https://github.com/StackStorm/orquesta.git@v1.5.0
pip3 install --user git+https://github.com/StackStorm/st2-rbac-backend.git@master
pip3 install --user greenlet==1.1.2
cd /tmp && \
  git clone https://github.com/StackStorm/st2 && \
  cd st2/st2common && \
  git checkout v3.8.0 && \
  sed -i 's/greenlet.*/greenlet/' requirements.txt && \
  pip3 install --user . && \
  cd .. && \
  rm -rf st2
cd -
```

You can also do this to a pyenv of your choice using its own `pip3`.
