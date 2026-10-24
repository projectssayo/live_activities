class create_account:
    def __init__(self, otp, ip, city, region, country, email):
        self.otp = otp
        self.ip = ip
        self.city = city
        self.region = region
        self.country = country
        self.email = email

    def get_html(self):
        return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Verify Your Email - sayoLabs</title>
</head>
<body style="margin:0; padding:0; background-color:#f2f4f7; font-family: Arial, Helvetica, sans-serif;">

<table width="100%" cellpadding="0" cellspacing="0" style="background:#f2f4f7; padding:20px 0;">
<tr>
<td align="center">

<table width="100%" cellpadding="0" cellspacing="0"
       style="max-width:600px; background:#ffffff; border-radius:10px; overflow:hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">

    <!-- Top Banner -->
    <tr>
        <td align="center">
            <img src="http://res.cloudinary.com/doeeoa5f1/image/upload/v1772456095/vad3nnmnhaoqclkaxdlt.png"
                 width="100%"
                 style="display:block; max-width:600px; height:auto;">
        </td>
    </tr>

    <!-- Main Content -->
    <tr>
        <td style="padding:35px 30px; text-align:center;">

            <h2 style="color:#2c3e50; margin:0 0 15px 0; font-size:24px;">
                Welcome to sayoLabs! 
            </h2>

            <p style="color:#555555; font-size:14px; margin:0 0 8px 0; line-height:1.6;">
                Please verify your email address to complete your account creation.
            </p>

            <p style="color:#777777; font-size:13px; margin:0 0 8px 0; line-height:1.6;">
                Use the One-Time Password below to verify your account.
            </p>

            <p style="color:#999999; font-size:12px; margin:0 0 25px 0; line-height:1.6;">
                This OTP is associated with: <strong style="color:#2c3e50;">{self.email}</strong>
            </p>

            <!-- OTP Code - Original Style -->
            <div style="display:inline-block;
                        background:#f0f4f8;
                        padding:18px 35px;
                        border-radius:8px;
                        font-size:32px;
                        font-weight:bold;
                        letter-spacing:8px;
                        color:#1f4e79;
                        font-family: 'Courier New', monospace;
                        margin-bottom: 10px;">
                {self.otp}
            </div>

            <p style="margin:15px 0 25px 0; font-size:13px; color:#777777;">
                ️ This OTP is valid for 5 minutes.
            </p>

            <!-- Divider -->
            <div style="border-bottom:1px solid #f0f2f5; margin:0 0 18px 0;"></div>

            <!-- Location Information - Elegantly Blended -->
            <div style="font-size:11px; color:#bbbbbb; font-weight:300; letter-spacing:0.2px; line-height:1.8; padding:2px 0;">
                <span style="color:#cccccc;">IP</span>
                <span style="color:#c5c5c5; margin:0 4px;">·</span>
                <span style="color:#bbbbbb;">{self.ip}</span>
                <span style="color:#dddddd; margin:0 6px;">|</span>
                <span style="color:#cccccc;">City</span>
                <span style="color:#c5c5c5; margin:0 4px;">·</span>
                <span style="color:#bbbbbb;">{self.city}</span>
                <span style="color:#dddddd; margin:0 6px;">|</span>
                <span style="color:#cccccc;">Region</span>
                <span style="color:#c5c5c5; margin:0 4px;">·</span>
                <span style="color:#bbbbbb;">{self.region}</span>
                <span style="color:#dddddd; margin:0 6px;">|</span>
                <span style="color:#cccccc;">Country</span>
                <span style="color:#c5c5c5; margin:0 4px;">·</span>
                <span style="color:#bbbbbb;">{self.country}</span>
            </div>

            <!-- Divider -->
            <div style="border-bottom:1px solid #f0f2f5; margin:18px 0 20px 0;"></div>

            <p style="margin:0; font-size:12px; color:#999999;">
                If you didn't request this, you can safely ignore this email.
            </p>

        </td>
    </tr>

    <!-- Divider -->
    <tr>
        <td style="border-top:1px solid #e5e5e5;"></td>
    </tr>

    <!-- Footer - Original Style -->
    <tr>
        <td style="padding:20px;">

            <table width="100%" cellpadding="0" cellspacing="0">
                <tr>

                    <!-- Logo -->
                    <td width="30%" align="left" style="vertical-align:top;">
                        <img src="http://res.cloudinary.com/doeeoa5f1/image/upload/v1772456392/cfjbscsueeqk79437f2b.png"
                             width="110"
                             style="display:block; max-width:110px; height:auto;">
                    </td>

                    <!-- Footer Text (Tight Spacing) -->
                    <td width="70%" align="right"
                        style="vertical-align:top; font-size:11px; line-height:14px; color:#8a8a8a;">

                        <div style="margin:0;">
                            Please do not reply directly to this email.
                        </div>

                        <div style="margin:0;">
                            © 2026 sayoLabs. All rights reserved.
                        </div>

                        <div style="margin:0;">
                            <a href="https://projectssayo.github.io/a/"
                               style="color:#8a8a8a; text-decoration:none;">
                               Contact Us
                            </a> |
                            <a href="https://projectssayo.github.io/b/"
                               style="color:#8a8a8a; text-decoration:none;">
                               Terms
                            </a> |
                            <a href="https://projectssayo.github.io/b/"
                               style="color:#8a8a8a; text-decoration:none;">
                               Privacy
                            </a>
                        </div>

                    </td>

                </tr>
            </table>

        </td>
    </tr>

</table>

</td>
</tr>
</table>

</body>
</html>
"""


class forgot_password:
    def __init__(self, otp, ip, city, region, country, email):
        self.otp = otp
        self.ip = ip
        self.city = city
        self.region = region
        self.country = country
        self.email = email

    def get_html(self):
        return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Password Reset Request - sayoLabs</title>
</head>
<body style="margin:0; padding:0; background-color:#f2f4f7; font-family: Arial, Helvetica, sans-serif;">

<table width="100%" cellpadding="0" cellspacing="0" style="background:#f2f4f7; padding:20px 0;">
<tr>
<td align="center">

<table width="100%" cellpadding="0" cellspacing="0"
       style="max-width:600px; background:#ffffff; border-radius:10px; overflow:hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">

    <!-- Top Banner -->
    <tr>
        <td align="center">
            <img src="https://res.cloudinary.com/dbiifyr5m/image/upload/v1781810716/test_uploads/eknd6oveib5g52wlhqud.png"
                 width="100%"
                 style="display:block; max-width:600px; height:auto;">
        </td>
    </tr>

    <!-- Main Content -->
    <tr>
        <td style="padding:35px 30px; text-align:center;">

            <h2 style="color:#2c3e50; margin:0 0 15px 0; font-size:24px;">
                Password Reset Request
            </h2>

            <p style="color:#555555; font-size:14px; margin:0 0 8px 0; line-height:1.6;">
                We received a request to reset the password for your sayoLabs account.
            </p>

            <p style="color:#777777; font-size:13px; margin:0 0 8px 0; line-height:1.6;">
                Use the One-Time Password below to reset your password.
            </p>

            <p style="color:#999999; font-size:12px; margin:0 0 25px 0; line-height:1.6;">
                This OTP is associated with: <strong style="color:#2c3e50;">{self.email}</strong>
            </p>

            <!-- OTP Code - Original Style -->
            <div style="display:inline-block;
                        background:#f0f4f8;
                        padding:18px 35px;
                        border-radius:8px;
                        font-size:32px;
                        font-weight:bold;
                        letter-spacing:8px;
                        color:#1f4e79;
                        font-family: 'Courier New', monospace;
                        margin-bottom: 10px;">
                {self.otp}
            </div>

            <p style="margin:15px 0 25px 0; font-size:13px; color:#777777;">
                ️ This OTP is valid for 5 minutes.
            </p>

            <!-- Divider -->
            <div style="border-bottom:1px solid #f0f2f5; margin:0 0 18px 0;"></div>

            <!-- Location Information - Elegantly Blended -->
            <div style="font-size:11px; color:#bbbbbb; font-weight:300; letter-spacing:0.2px; line-height:1.8; padding:2px 0;">
                <span style="color:#cccccc;">IP</span>
                <span style="color:#c5c5c5; margin:0 4px;">·</span>
                <span style="color:#bbbbbb;">{self.ip}</span>
                <span style="color:#dddddd; margin:0 6px;">|</span>
                <span style="color:#cccccc;">City</span>
                <span style="color:#c5c5c5; margin:0 4px;">·</span>
                <span style="color:#bbbbbb;">{self.city}</span>
                <span style="color:#dddddd; margin:0 6px;">|</span>
                <span style="color:#cccccc;">Region</span>
                <span style="color:#c5c5c5; margin:0 4px;">·</span>
                <span style="color:#bbbbbb;">{self.region}</span>
                <span style="color:#dddddd; margin:0 6px;">|</span>
                <span style="color:#cccccc;">Country</span>
                <span style="color:#c5c5c5; margin:0 4px;">·</span>
                <span style="color:#bbbbbb;">{self.country}</span>
            </div>

            <!-- Divider -->
            <div style="border-bottom:1px solid #f0f2f5; margin:18px 0 20px 0;"></div>

            <p style="margin:0; font-size:12px; color:#999999;">
                If you didn't request a password reset, you can safely ignore this email.
                Your password will not be changed unless you use the OTP above.
            </p>

        </td>
    </tr>

    <!-- Divider -->
    <tr>
        <td style="border-top:1px solid #e5e5e5;"></td>
    </tr>

    <!-- Footer - Original Style -->
    <tr>
        <td style="padding:20px;">

            <table width="100%" cellpadding="0" cellspacing="0">
                <tr>

                    <!-- Logo -->
                    <td width="30%" align="left" style="vertical-align:top;">
                        <img src="http://res.cloudinary.com/doeeoa5f1/image/upload/v1772456392/cfjbscsueeqk79437f2b.png"
                             width="110"
                             style="display:block; max-width:110px; height:auto;">
                    </td>

                    <!-- Footer Text (Tight Spacing) -->
                    <td width="70%" align="right"
                        style="vertical-align:top; font-size:11px; line-height:14px; color:#8a8a8a;">

                        <div style="margin:0;">
                            Please do not reply directly to this email.
                        </div>

                        <div style="margin:0;">
                            © 2026 sayoLabs. All rights reserved.
                        </div>

                        <div style="margin:0;">
                            <a href="https://projectssayo.github.io/a/"
                               style="color:#8a8a8a; text-decoration:none;">
                               Contact Us
                            </a> |
                            <a href="https://projectssayo.github.io/b/"
                               style="color:#8a8a8a; text-decoration:none;">
                               Terms
                            </a> |
                            <a href="https://projectssayo.github.io/b/"
                               style="color:#8a8a8a; text-decoration:none;">
                               Privacy
                            </a>
                        </div>

                    </td>

                </tr>
            </table>

        </td>
    </tr>

</table>

</td>
</tr>
</table>

</body>
</html>
"""
