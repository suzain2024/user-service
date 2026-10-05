package com.example.user_service.dto;

import lombok.Data;

@Data
public class LoginRequestDto {
    private String password;
    private String email;

}
