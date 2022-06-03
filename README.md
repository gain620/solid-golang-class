# solid-golang-class 1주차 강의 소스코드

소프트웨어 엔지니어링의 개념 이해
- 프로그래밍과 소프트웨어 엔지니어링의 차이

02
Go 실무 예제로 보는 SOLID 개발 패턴 이해
- Single responsibility: 단일 책임 원칙
- Open・Closed: 개방・폐쇄 원칙
- Liskov substitution: 리스코프 치환 원칙
- Interface segregation: 인터페이스 분리 원칙
- Dependency inversion: 의존 관계 역전 원칙

03
Go 데이터 처리 소프트웨어 개발 실무 1: 이벤트 기반 데이터 가져오기
- Consumer・Processor・Storage Provider
- Consumer Component 인터페이스 정의
- Kafka Consumer 객체 구현
- Consumer 객체 메소드 테스트
- 이벤트 기반 데이터 처리 시뮬레이션
- 적용된 SOLID 원칙 설명

과제
RabbitMQ Consumer 객체 구현
- [X] 정의한 인터페이스를 구현하기
- [X] 구현된 객체에서 처리해야 할 기능 구현하기
- [X] 구현된 객체 메소드 단위 테스트 작성하기
- [ ] RabbitMQ InitDeliveryChannel 이슈 해결
- [X] SOLID 개발 패턴 적용 설명