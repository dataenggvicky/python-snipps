# Adding Python Packages and Files for AWS Glue and Lambda

This guide explains methods to include additional Python packages and files in AWS Lambda and Glue jobs.

## AWS Lambda

### Adding Python Packages

#### 1. **Using a Deployment Package**
- **Steps**:
  1. Create a virtual environment:
     ```bash
     python -m venv myenv
     source myenv/bin/activate
     ```
  2. Install packages compatible with Lambda's Linux environment:
     ```bash
     pip install --platform manylinux2014_x86_64 --target ./packages --implementation cp --python-version 3.9 --only-binary=:all: <package-name>
     ```
  3. Zip the package with your Lambda handler:
     ```bash
     cd packages
     zip -r ../lambda_function.zip .
     cd ..
     zip lambda_function.zip lambda_function.py
     ```
  4. Deploy the ZIP via AWS Console, CLI, or SDK.

#### 2. **Using Lambda Layers**
- **Steps**:
  1. Package dependencies into a `python` directory:
     ```bash
     mkdir -p python/lib/python3.9/site-packages
     pip install -t python/lib/python3.9/site-packages -r requirements.txt
     zip -r layer.zip python
     ```
  2. Create a Lambda layer via AWS Console/CLI:
     ```bash
     aws lambda publish-layer-version --layer-name my-layer --zip-file fileb://layer.zip
     ```
  3. Attach the layer to your Lambda function.

#### 3. **Using Container Image**
- **Steps**:
  1. Create a `Dockerfile`:
     ```dockerfile
     FROM public.ecr.aws/lambda/python:3.9
     COPY requirements.txt .
     RUN pip install -r requirements.txt
     COPY lambda_function.py .
     CMD ["lambda_function.handler"]
     ```
  2. Build, tag, and push the image to Amazon ECR.
  3. Deploy the image to Lambda.

### Adding Additional Python Files
- **Include in Deployment Package**: Place files in the root directory and import them.
- **Layers**: Store files in the layer ZIP and reference them using absolute paths (e.g., `/opt/python/my_file.py`).

---

## AWS Glue

### Adding Python Packages

#### 1. **For Glue Spark Jobs**
- **Method 1**: Use `--additional-python-modules` in job parameters:
  ```bash
  # Example: Install specific versions
  --additional-python-modules "numpy==1.21.2,s3://bucket/path/to/mypackage.whl"