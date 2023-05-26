 #!/bin/sh
echo 'Installing dependencies from "requirements.txt" into virtual environment'
python -m pip install -r scripts/requirements.txt

# install wkhtmltopdf
# # TODO: Need to handle other OSes
# echo 'Installing wkhtmltopdf required for pdf conversion'
# unameOut="$(uname -s)"
# case "${unameOut}" in
#     Linux*)     sudo apt-get install wkhtmltopdf -y;;
#     Darwin*)    brew install wkhtmltopdf;;
#     *)          machine="UNKNOWN:${unameOut}"
# esac

echo 'Running "prepdocs.py"'
python ./scripts/prepdocs.py -v