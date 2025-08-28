def get_sftp():
    print('sftp 작업을 시작합니다')
    
# *args 사용
def regist(name, sex, *args):
    print(f'이름: {name}')
    print(f'성별: {sex}')
    print(f'기타 옵션들: {args}')

# *args, **kwargs 같이 사용
def regist2(name, sex, *args, **kwargs):
    print(f'이름: {name}')
    print(f'성별: {sex}')
    print(f'기타 옵션들: {args}')

    email = kwargs['email'] or None # kwargs.get('email') 이라고 작성한 것과 동일함 -> kwargs['email'] 로 작성 시, key 값에 'email'이 없으면 에러 발생
    phone = kwargs['phone'] or None # kwargs.get('phone') 이라고 작성한 것과 동일함 -> kwargs['phone'] 로 작성 시, key 값에 'phone'이 없으면 에러 발생
    if email:
        print(email)
    if phone:
        print(phone)