#!/bin/bash
set -e
echo "Compiling backend.."
echo "++ compiling bfile module.."
(cd ring/bfile; make)
echo "++ compiling Ringo.."
(cd ring/; ./compile.sh)
echo "Compiling frontend.."
(cd ringogw/; ./compile.sh)
echo "All ok!"
