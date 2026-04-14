import cv2
import mediapipe as mp
import numpy as np

mp_face_mesh = mp.solutions.face_mesh
mp_drawing = mp.solutions.drawing_utils
mp_styles = mp.solutions.drawing_styles


def calc_dist(p1, p2, w, h):
    x1, y1 = int(p1.x * w), int(p1.y * h)
    x2, y2 = int(p2.x * w), int(p2.y * h)
    return np.hypot(x2 - x1, y2 - y1)


def detectar_emocao(face_landmarks, w, h):
    """
    Heurística simples de expressão facial:
    - sorriso: boca larga
    - surpresa: boca muito aberta
    - neutro: caso contrário
    [web:474][web:479]
    """
    # índices da malha do MediaPipe Face Mesh (modelo padrão)
    # boca
    mouth_left = 61
    mouth_right = 291
    mouth_top = 13
    mouth_bottom = 14

    lm = face_landmarks.landmark

    # distâncias horizontais e verticais da boca
    boca_largura = calc_dist(lm[mouth_left], lm[mouth_right], w, h)
    boca_altura = calc_dist(lm[mouth_top], lm[mouth_bottom], w, h)

    # razão altura/largura para normalizar
    if boca_largura == 0:
        return "neutro"

    razao_abertura = boca_altura / boca_largura

    # thresholds escolhidos empiricamente; ajuste olhando na webcam
    if razao_abertura > 0.5:
        return "surpreso"
    elif boca_largura > 120 and razao_abertura > 0.25:
        return "feliz"
    else:
        return "neutro"


def main():
    cap = cv2.VideoCapture(0)
    if not cap.isOpened():
        print("Erro ao abrir webcam.")
        return

    # Face Mesh de alta qualidade: refine_landmarks=True melhora olhos/iris [web:471][web:477]
    with mp_face_mesh.FaceMesh(
        max_num_faces=1,
        refine_landmarks=True,
        min_detection_confidence=0.6,
        min_tracking_confidence=0.6,
    ) as face_mesh:

        while True:
            ret, frame = cap.read()
            if not ret:
                print("Erro ao capturar frame.")
                break

            h, w, _ = frame.shape

            # MediaPipe trabalha em RGB
            rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            results = face_mesh.process(rgb)

            emotion_text = "sem rosto"

            if results.multi_face_landmarks:
                for face_landmarks in results.multi_face_landmarks:
                    # desenhar malha do rosto
                    mp_drawing.draw_landmarks(
                        image=frame,
                        landmark_list=face_landmarks,
                        connections=mp_face_mesh.FACEMESH_TESSELATION,
                        landmark_drawing_spec=None,
                        connection_drawing_spec=mp_styles
                        .get_default_face_mesh_tesselation_style(),
                    )

                    mp_drawing.draw_landmarks(
                        image=frame,
                        landmark_list=face_landmarks,
                        connections=mp_face_mesh.FACEMESH_CONTOURS,
                        landmark_drawing_spec=None,
                        connection_drawing_spec=mp_styles
                        .get_default_face_mesh_contours_style(),
                    )

                    # detectar expressão simples
                    emotion_text = detectar_emocao(face_landmarks, w, h)

            # desenha uma caixa de texto no topo
            cv2.rectangle(frame, (0, 0), (250, 40), (0, 0, 0), -1)
            cv2.putText(
                frame,
                f"Emocao: {emotion_text}",
                (10, 28),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.7,
                (0, 255, 255),
                2,
                cv2.LINE_AA,
            )

            cv2.imshow("Mapa de Rosto + Emocao (simples)", frame)

            if cv2.waitKey(1) & 0xFF == ord("q"):
                break

    cap.release()
    cv2.destroyAllWindows()


if __name__ == "__main__":
    main()