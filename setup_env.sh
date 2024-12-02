#!/bin/bash

echo "                                                                                 *        *        *        __o    *       *"
echo "                                                                              *      *       *        *    /_| _     *"
echo "                                                                                 K  *     K      *        O'_)/ \  *    *"
echo "                                                                                <')____  <')____    __*   V   \  ) __  *"
echo "                                                                                 \ ___ )--\ ___ )--( (    (___|__)/ /*     *"
echo "                                                                               *  |   |    |   |  * \ \____| |___/ /  *"
echo "                                                                                  |*  |    |   | aos \____________/       *"
echo "____   ____.__        __                         .__            __             .___       __         .__                         __"
echo "\   \ /   /|__|______|  | __  __________   _____ |  |__   _____/  |_  ______ __| _/____ _/  |______  |  | _____     ____   _____/  |_"
echo " \   Y   / |  \_  __ \  |/ / /  ___/  _ \ /     \|  |  \_/ __ \   __\/  ___// __ |\__  \\   __\__  \ |  | \__  \   / ___\_/ __ \   __\\"
echo "  \     /  |  ||  | \/    <  \___ (  <_> )  Y Y  \   Y  \  ___/|  |  \___ \/ /_/ | / __ \|  |  / __ \|  |__/ __ \_/ /_/  >  ___/|  |"
echo "   \___/   |__||__|  |__|_ \/____  >____/|__|_|  /___|  /\___  >__| /____  >____ |(____  /__| (____  /____(____  /\___  / \___  >__|"
echo "                          \/     \/            \/     \/     \/          \/     \/     \/          \/          \//_____/      \/"

if [ ! -f .venv/bin/pip ]; then
  make install
fi

if [ -f .venv/bin/pip ]; then
  source .venv/bin/activate
  dependency_diff=$(grep -Fvf <(pip freeze) requirements-lock.txt)
fi

if [[ ! -z "$dependency_diff" ]]; then
  echo deps diff: $dependency_diff
  make install
fi

#Snowbird
. ./infrastructure/auth.sh

#DBT
. ./dbt/auth.sh

code .
