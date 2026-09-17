package com.bai1.dto;

public class Banner {

    private Long id;
    private String title;
    private String imageUrl;
    private String targetUrl;
    private String description;
    private boolean active;

    public Banner() {
    }

    public Banner(Long id, String title, String imageUrl, String targetUrl, String description, boolean active) {
        this.id = id;
        this.title = title;
        this.imageUrl = imageUrl;
        this.targetUrl = targetUrl;
        this.description = description;
        this.active = active;
    }

    /**
     * Tạo Banner mặc định khi Promotion Service gặp sự cố hoặc timeout (> 2s)
     * Đáp ứng yêu cầu nghiệp vụ: thông báo "Khuyến mãi đang được cập nhật"
     */
    public static Banner defaultBanner() {
        return new Banner(
                0L,
                "Khuyến mãi đang được cập nhật",
                "/images/banners/default-promotion.png",
                "/promotions/default",
                "Chương trình khuyến mãi đang được cập nhật. Vui lòng quay lại sau!",
                true
        );
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getImageUrl() {
        return imageUrl;
    }

    public void setImageUrl(String imageUrl) {
        this.imageUrl = imageUrl;
    }

    public String getTargetUrl() {
        return targetUrl;
    }

    public void setTargetUrl(String targetUrl) {
        this.targetUrl = targetUrl;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    @Override
    public String toString() {
        return "Banner{" +
                "id=" + id +
                ", title='" + title + '\'' +
                ", imageUrl='" + imageUrl + '\'' +
                ", targetUrl='" + targetUrl + '\'' +
                ", description='" + description + '\'' +
                ", active=" + active +
                '}';
    }
}
