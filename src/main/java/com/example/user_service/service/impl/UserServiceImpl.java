package com.example.user_service.service.impl;

import com.example.user_service.dto.AuthResponseDto;
import com.example.user_service.dto.LoginRequestDto;
import com.example.user_service.dto.UserRequestDto;
import com.example.user_service.dto.UserResponseDto;
import com.example.user_service.entity.UserEntity;
import com.example.user_service.repository.UserRepository;
import com.example.user_service.service.JwtService;
import com.example.user_service.service.TokenBlacklistService;
import com.example.user_service.service.UserService;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class UserServiceImpl implements UserService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtService jwtService;
    private final TokenBlacklistService tokenBlacklistService;

    public UserServiceImpl(UserRepository userRepository,
                           PasswordEncoder passwordEncoder,
                           JwtService jwtService,
                           TokenBlacklistService tokenBlacklistService) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        this.jwtService = jwtService;
        this.tokenBlacklistService = tokenBlacklistService;
    }

    @Override
    public UserResponseDto createUser(UserRequestDto userDto) {
        UserEntity user = new UserEntity();
        user.setName(userDto.getName());
        user.setEmail(userDto.getEmail());
        user.setPassword(passwordEncoder.encode(userDto.getPassword()));
        user.setRole(userDto.getRole() != null && !userDto.getRole().isBlank() ? userDto.getRole() : "ROLE_USER");

        UserEntity savedUser = userRepository.save(user);
        UserResponseDto response = new UserResponseDto();
        response.setEmail(savedUser.getEmail());
        response.setId(savedUser.getId());
        response.setName(savedUser.getName());
        return response;
    }

    @Override
    public AuthResponseDto login(LoginRequestDto request) {
        UserEntity user = userRepository.findByEmail(request.getEmail())
                .orElseThrow(() -> new RuntimeException("Invalid email or password"));

        if (!passwordEncoder.matches(request.getPassword(), user.getPassword())) {
            throw new RuntimeException("Invalid email or password");
        }

        String role = user.getRole() != null ? user.getRole() : "ROLE_USER";
        String token = jwtService.generateToken(user.getId(), user.getEmail(), role);
        return new AuthResponseDto(token);
    }

    @Override
    public List<UserResponseDto> getAllUsers()
    {
        List<UserEntity> users=userRepository.findAll();
        List<UserResponseDto> responseList=new ArrayList<>();
        for(UserEntity user:users)
        {
            UserResponseDto response=new UserResponseDto();
            response.setId(user.getId());
            response.setEmail(user.getEmail());
            response.setName(user.getName());
            responseList.add(response);
        }
        return responseList;
    }
    @Override
    @Cacheable(value = "users",key = "#id")
    public UserResponseDto getUserById(Long id)
    {
        UserEntity user=userRepository.findById(id).orElseThrow(()->new RuntimeException("User not found"+id));
        UserResponseDto newUser=new UserResponseDto();
        newUser.setId(user.getId());
        newUser.setEmail(user.getEmail());
        newUser.setName(user.getName());
        return newUser;
    }
    @Override
    @CacheEvict(value = "users",key = "#id")
    public UserResponseDto deleteUser(Long id)
    {
       UserEntity user= userRepository.findById(id).orElseThrow(()->new RuntimeException("User not found"+id));
        userRepository.delete(user);
        UserResponseDto newUser=new UserResponseDto();
        newUser.setId(user.getId());
        newUser.setEmail(user.getEmail());
        newUser.setName(user.getName());
        return newUser;
    }
    @Override
    @CacheEvict(value = "users",key = "#id")
    public UserResponseDto updateUserById(Long id,UserRequestDto userDto)
    {
        UserEntity user=userRepository.findById(id).orElseThrow(()->new RuntimeException("User not found"+id));
        user.setName(userDto.getName());
        user.setEmail(userDto.getEmail());
        UserEntity updatedUser=userRepository.save(user);
        UserResponseDto newUser=new UserResponseDto();
        newUser.setId(user.getId());
        newUser.setEmail(user.getEmail());
        newUser.setName(user.getName());
        return newUser;
    }
    @Override
    public void logout(String token) {
        this.tokenBlacklistService.blacklistToken(token);
    }
}
