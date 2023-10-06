# python setup.py sdist bdist_wheel
# python -m twine upload dist/*
#!/bin/bash

# 입력 문자열
current_version=$(cat "owlman/__init__.py" | grep -o -E "(\d+)\.(\d+)\.(\d+)")
echo "PREV : $current_version"

# '.'을 기준으로 문자열을 분리
IFS='.' read -ra version_parts <<< "$current_version"

case $1 in
    'major')
    # 마지막 숫자를 가져와서 1 증가시킴
    first_part="${version_parts[0]}"
    ((first_part++))
    new_version="$first_part.0.0"
    ;;
    'minor')
    middle_part="${version_parts[1]}"
    ((middle_part++))
    new_version="${version_parts[0]}.$middle_part.0"
    ;;
    *)
    last_part="${version_parts[2]}"
    ((last_part++))
    new_version="${version_parts[0]}.${version_parts[1]}.$last_part"
    ;;
esac

echo "__version__ = '$new_version'"  > "owlman/__init__.py"

pip freeze > requirements.txt
rm -rf build
rm -rf dist
python setup.py sdist bdist_wheel
python -m twine upload dist/*
git add .
git commit -m "$new_version"
git push

echo "NEXT : $new_version"
