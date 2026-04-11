#!/bin/bash
set -e

# 0. Go 환경 변수 설정 확인
export GOPATH=$(go env GOPATH)
export PATH=$PATH:$GOPATH/bin

echo "1/4: 의존성 패키지(goimports, gopls) 설치 중..."

go install golang.org/x/tools/cmd/goimports@latest
go install golang.org/x/tools/gopls@latest

echo "2/4: gonb 바이너리 설치 중..."
go install github.com/janpfeifer/gonb@latest

echo "3/4: Jupyter에 gonb 커널 등록 중..."
# PATH를 명시적으로 전달하여 gonb 명령어를 찾을 수 있게 함
gonb --install

echo "4/4: 설치된 커널 목록 확인..."
jupyter kernelspec list

echo "===================================================="
echo "gonb 커널 설치 및 등록이 완료되었습니다."
echo "Jupyter Notebook 페이지를 새로고침(F5) 하거나,"
echo "커널 메뉴에서 'Change Kernel' -> 'Go (gonb)'를 선택하세요."
echo "===================================================="