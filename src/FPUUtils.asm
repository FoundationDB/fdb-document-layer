; Routines to convert 80 unsigned char buffer aka long double to double and vice versa
;

PUBLIC	convert64dto80
PUBLIC	convert80to64d

PUBLIC	convert64ito80
PUBLIC	convert80to64i

PUBLIC	add80to80
PUBLIC	multiply80to80

_TEXT	SEGMENT

convert64dto80 PROC						; void __fastcall convert64dto80(unsigned char* value, unsigned char* result);
	FLD QWORD PTR [RCX]					; load RXC(value) into FPU Stack
	FSTP TBYTE PTR [RDX]				; pop up from FPU stack and store into RDX(result)
	RET	0
convert64dto80 ENDP

convert80to64d PROC						; double __fastcall convert80to64d(unsigned char *value, unsigned char* result)
	FLD TBYTE PTR[RCX]					; load RCX(value) into FPU stack
	FSTP QWORD PTR[RDX]					; pop up from FPU stack into RDX(result) as 64 bit double
	RET	0
convert80to64d ENDP


convert64ito80 PROC						; void __fastcall convert64ito80(unsigned char* value, unsigned char* result);
	FILD QWORD PTR[RCX]					; load RCX(value) into FPU Stack as 64 bit integer
	FSTP TBYTE PTR [RDX]				; pop up from FPU stack and store into RDX(result)
	RET	0
convert64ito80 ENDP

convert80to64i PROC						; double __fastcall convert80to64i(unsigned char *value, unsigned char* result)
	FLD TBYTE PTR[RCX]					; load RCX(value) into FPU stack
	FISTP QWORD PTR[RDX]				; pop up from FPU stack into RDX(result) as 64 bit integer
	RET	0
convert80to64i ENDP


add80to80 PROC							; void __fastcall add80to80(unsigned char* ldA, unsigned char* ldB, unsigned char* result);
	FLD TBYTE PTR [RCX]					; load RCX(ldA) and push it onto the FPU stack
	FLD TBYTE PTR [RDX]					; load RDX(ldB) and push it onto the fpu stack
	FADD								; add st(0) to st(1) and result now in st(0)
	FSTP TBYTE PTR [R8]					; pop st(0) from the stack and store it in R8(result)
	RET 0
add80to80 ENDP;


multiply80to80 PROC						; void __fastcall multiply80to80(unsigned char* ldA, unsigned char* ldB, unsigned char* result);
	FLD TBYTE PTR [RCX]					; load ldA and push it onto the fpu stack
	FLD TBYTE PTR [RDX]					; load ldB and push it onto the fpu stack
	FMUL								; multiply st(0) to st(1) and pop (result now in st(0))
	FSTP TBYTE PTR [R8]					; pop st(0) from the stack and store it in R8
	RET 0
multiply80to80 ENDP;


_TEXT	ENDS
END
