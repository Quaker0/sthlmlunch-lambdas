.PHONY: setup build deploy invoke run clean

setup:
	pip install --target ./package -r requirements.txt --upgrade

clean:
	rm -f function.zip
	rm -f output.json
	rm -rf package

build: setup
	rm -f function.zip
	black lambda_function.py
	cd package && zip -r9 ~/sthlmlunch-lambdas/statistics/function.zip . && cd ..
	zip -g function.zip lambda_function.py
	chmod u=rwx,go=r function.zip

deploy:
	aws lambda update-function-code --region eu-north-1 --function-name statistics --zip-file fileb://function.zip

invoke:
	aws lambda invoke --function-name statistics output.json
	@ jq . output.json

run: setup
	@ tput setaf 3; source ".env" && python3 -c "import lambda_function; import json; print(json.dumps(lambda_function.lambda_handler(None, None), indent=2))"
