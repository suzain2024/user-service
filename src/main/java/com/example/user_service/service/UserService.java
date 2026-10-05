package com.example.user_service.service;

import com.example.user_service.dto.AuthResponseDto;
import com.example.user_service.dto.LoginRequestDto;
import com.example.user_service.dto.UserRequestDto;
import com.example.user_service.dto.UserResponseDto;

import java.util.List;
//we are using this interface which means this business logic are must as
// there are api related to each so ony we need to update the requirement as per user which is defined in UserServiceImpl
public interface UserService {
    UserResponseDto createUser(UserRequestDto user);
  List<UserResponseDto> getAllUsers();
  UserResponseDto getUserById(Long id);
  UserResponseDto updateUserById(Long id,UserRequestDto user);
  UserResponseDto deleteUser(Long id);
    void logout(String token);
    AuthResponseDto login(LoginRequestDto loginRequest);
}
